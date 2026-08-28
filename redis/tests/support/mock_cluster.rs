use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, LazyLock, RwLock},
    time::Duration,
};

use redis::{
    FromRedisValue, ServerErrorKind,
    cluster::{self, ClusterClient, ClusterClientBuilder},
};

use redis::{IntoConnectionInfo, RedisResult, Value};

#[cfg(feature = "cluster-async")]
use redis::{RedisFuture, aio, cluster_async};

#[cfg(feature = "cluster-async")]
use futures::future;

#[cfg(feature = "cluster-async")]
use tokio::runtime::Runtime;

type Handler = Arc<dyn Fn(&[u8], u16) -> Result<(), RedisResult<Value>> + Send + Sync>;

static HANDLERS: LazyLock<RwLock<HashMap<String, Handler>>> = LazyLock::new(Default::default);

/// One packed write: the port it went to, and how many commands it carried.
type PipelineWrite = (u16, usize);

/// Every packed write the sync cluster pipeline path has made, per mock cluster
/// name.
static PIPELINE_WRITES: LazyLock<RwLock<HashMap<String, Vec<PipelineWrite>>>> =
    LazyLock::new(Default::default);

/// Scripted results for successive packed pipeline writes, per mock cluster.
/// Missing entries and exhausted scripts mean success.
static PIPELINE_SEND_RESULTS: LazyLock<RwLock<HashMap<String, VecDeque<RedisResult<()>>>>> =
    LazyLock::new(Default::default);

/// Take the packed pipeline writes recorded for `name` since the last call, in
/// the order they were written, as `(port, commands in that write)`.
///
/// There is one entry per `send_packed_command`, so a pipeline that is retried
/// as a pipeline shows up as a single wide write, while one retried command by
/// command would not show up here at all: single-command requests go through
/// `req_command` instead.
pub fn take_pipeline_writes(name: &str) -> Vec<PipelineWrite> {
    PIPELINE_WRITES
        .write()
        .unwrap()
        .remove(name)
        .unwrap_or_default()
}

/// Set the results returned by successive `send_packed_command` calls.
pub fn set_pipeline_send_results(name: &str, results: Vec<RedisResult<()>>) {
    PIPELINE_SEND_RESULTS
        .write()
        .unwrap()
        .insert(name.to_string(), results.into());
}

#[derive(Clone)]
pub struct MockConnection {
    pub handler: Handler,
    pub port: u16,
    /// The mock cluster this connection belongs to, used to record its writes.
    name: String,
    /// Commands written by `send_packed_command` that `recv_response` has not
    /// handed back yet. The cluster pipeline path writes a whole pipe and then
    /// reads one response per command, so replaying the commands in order lets a
    /// handler see which command each response belongs to.
    pending: VecDeque<Vec<u8>>,
}

#[cfg(feature = "cluster-async")]
impl cluster_async::Connect for MockConnection {
    fn connect_with_config<'a, T>(
        info: T,
        _config: redis::AsyncConnectionConfig,
    ) -> RedisFuture<'a, Self>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        let info = info.into_connection_info().unwrap();

        let (name, port) = match &info.addr() {
            redis::ConnectionAddr::Tcp(addr, port) => (addr, *port),
            _ => unreachable!(),
        };
        Box::pin(future::ok(MockConnection {
            handler: HANDLERS
                .read()
                .unwrap()
                .get(name)
                .unwrap_or_else(|| panic!("Handler `{name}` were not installed"))
                .clone(),
            port,
            name: name.clone(),
            pending: VecDeque::new(),
        }))
    }
}

impl cluster::Connect for MockConnection {
    fn connect<'a, T>(info: T, _timeout: Option<Duration>) -> RedisResult<Self>
    where
        T: IntoConnectionInfo,
    {
        let info = info.into_connection_info().unwrap();

        let (name, port) = match &info.addr() {
            redis::ConnectionAddr::Tcp(addr, port) => (addr, *port),
            _ => unreachable!(),
        };
        Ok(MockConnection {
            handler: HANDLERS
                .read()
                .unwrap()
                .get(name)
                .unwrap_or_else(|| panic!("Handler `{name}` were not installed"))
                .clone(),
            port,
            name: name.clone(),
            pending: VecDeque::new(),
        })
    }

    fn send_packed_command(&mut self, cmd: &[u8]) -> RedisResult<()> {
        let commands = split_packed_commands(cmd);
        PIPELINE_WRITES
            .write()
            .unwrap()
            .entry(self.name.clone())
            .or_default()
            .push((self.port, commands.len()));
        let result = PIPELINE_SEND_RESULTS
            .write()
            .unwrap()
            .get_mut(&self.name)
            .and_then(VecDeque::pop_front)
            .unwrap_or(Ok(()));
        if result.is_ok() {
            self.pending.extend(commands);
        }
        result
    }

    fn set_write_timeout(&self, _dur: Option<std::time::Duration>) -> RedisResult<()> {
        Ok(())
    }

    fn set_read_timeout(&self, _dur: Option<std::time::Duration>) -> RedisResult<()> {
        Ok(())
    }

    fn recv_response(&mut self) -> RedisResult<Value> {
        // Drive responses from the handler so cluster-pipeline tests can inject inline per-command replies (e.g. `MOVED`).
        // Invoked only by the cluster pipeline receive path. The command replayed
        // here is the one this response answers, so handlers can branch on the
        // command as well as on the port.
        let cmd = self.pending.pop_front().unwrap_or_default();
        (self.handler)(&cmd, self.port).expect_err("Handler did not specify a response")
    }
}

/// Split a packed pipeline back into the commands that were written into it.
///
/// The cluster pipeline path packs many commands into a single write and then
/// reads the responses one at a time, so the mock has to undo the packing to
/// pair each response with its command.
fn split_packed_commands(buf: &[u8]) -> Vec<Vec<u8>> {
    fn read_line(buf: &[u8]) -> Option<(&[u8], &[u8])> {
        let end = buf.windows(2).position(|window| window == b"\r\n")?;
        Some((&buf[..end], &buf[end + 2..]))
    }

    fn read_len(buf: &[u8], prefix: u8) -> Option<(usize, &[u8])> {
        let (line, rest) = read_line(buf)?;
        let digits = line.strip_prefix(&[prefix])?;
        let len = std::str::from_utf8(digits).ok()?.parse().ok()?;
        Some((len, rest))
    }

    let mut cmds = Vec::new();
    let mut rest = buf;
    while !rest.is_empty() {
        let start = rest;
        let Some((args, after_header)) = read_len(rest, b'*') else {
            break;
        };
        rest = after_header;
        for _ in 0..args {
            let Some((len, after_len)) = read_len(rest, b'$') else {
                return cmds;
            };
            if after_len.len() < len + 2 {
                return cmds;
            }
            rest = &after_len[len + 2..];
        }
        cmds.push(start[..start.len() - rest.len()].to_vec());
    }
    cmds
}

pub fn contains_slice(xs: &[u8], ys: &[u8]) -> bool {
    for i in 0..xs.len() {
        if xs[i..].starts_with(ys) {
            return true;
        }
    }
    false
}

pub fn is_connection_check(cmd: &[u8]) -> bool {
    contains_slice(cmd, b"READONLY") || contains_slice(cmd, b"PING")
}

pub fn respond_startup(name: &str, cmd: &[u8]) -> Result<(), RedisResult<Value>> {
    if is_connection_check(cmd) {
        Err(Ok(Value::SimpleString("OK".into())))
    } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
        Err(Ok(Value::Array(vec![Value::Array(vec![
            Value::Int(0),
            Value::Int(16383),
            Value::Array(vec![
                Value::BulkString(name.as_bytes().to_vec()),
                Value::Int(6379),
            ]),
        ])])))
    } else {
        Ok(())
    }
}

#[derive(Clone)]
pub struct MockSlotRange {
    pub primary_port: u16,
    pub replica_ports: Vec<u16>,
    pub slot_range: std::ops::Range<u16>,
}

pub fn respond_startup_with_replica(name: &str, cmd: &[u8]) -> Result<(), RedisResult<Value>> {
    respond_startup_with_replica_using_config(name, cmd, None)
}

pub fn respond_startup_two_nodes(name: &str, cmd: &[u8]) -> Result<(), RedisResult<Value>> {
    respond_startup_with_replica_using_config(
        name,
        cmd,
        Some(vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![],
                slot_range: (0..8191),
            },
            MockSlotRange {
                primary_port: 6380,
                replica_ports: vec![],
                slot_range: (8192..16383),
            },
        ]),
    )
}

pub fn respond_startup_with_replica_using_config(
    name: &str,
    cmd: &[u8],
    slots_config: Option<Vec<MockSlotRange>>,
) -> Result<(), RedisResult<Value>> {
    let slots_config = slots_config.unwrap_or(vec![
        MockSlotRange {
            primary_port: 6379,
            replica_ports: vec![6380],
            slot_range: (0..8191),
        },
        MockSlotRange {
            primary_port: 6381,
            replica_ports: vec![6382],
            slot_range: (8192..16383),
        },
    ]);
    if is_connection_check(cmd) {
        Err(Ok(Value::SimpleString("OK".into())))
    } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
        let slots = slots_config
            .into_iter()
            .map(|slot_config| {
                let mut entry = vec![
                    Value::Int(slot_config.slot_range.start as i64),
                    Value::Int(slot_config.slot_range.end as i64),
                    Value::Array(vec![
                        Value::BulkString(name.as_bytes().to_vec()),
                        Value::Int(slot_config.primary_port as i64),
                    ]),
                ];
                for replica_port in slot_config.replica_ports {
                    entry.push(Value::Array(vec![
                        Value::BulkString(name.as_bytes().to_vec()),
                        Value::Int(replica_port as i64),
                    ]));
                }
                Value::Array(entry)
            })
            .collect();
        Err(Ok(Value::Array(slots)))
    } else {
        Ok(())
    }
}

#[cfg(feature = "cluster-async")]
impl aio::ConnectionLike for MockConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a redis::Cmd) -> RedisFuture<'a, Value> {
        Box::pin(future::ready(
            (self.handler)(&cmd.get_packed_command(), self.port)
                .expect_err("Handler did not specify a response"),
        ))
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        _offset: usize,
        _count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(future::ready(
            pipeline
                .cmd_iter()
                .map(|cmd| {
                    (self.handler)(&cmd.get_packed_command(), self.port)
                        .expect_err("Handler did not specify a response")
                })
                .collect(),
        ))
    }

    fn get_db(&self) -> i64 {
        0
    }
}

impl redis::ConnectionLike for MockConnection {
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        (self.handler)(cmd, self.port).expect_err("Handler did not specify a response")
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        _count: usize,
    ) -> RedisResult<Vec<Value>> {
        let res = (self.handler)(cmd, self.port).expect_err("Handler did not specify a response");
        match res {
            Err(err) => Err(err),
            Ok(res) => {
                if let Value::Array(results) = res {
                    match results.into_iter().nth(offset) {
                        Some(Value::Array(res)) => Ok(res),
                        _ => Err(
                            (ServerErrorKind::ResponseError.into(), "non-array response").into(),
                        ),
                    }
                } else {
                    Err((
                        ServerErrorKind::ResponseError.into(),
                        "non-array response",
                        String::from_redis_value(res).unwrap(),
                    )
                        .into())
                }
            }
        }
    }

    fn get_db(&self) -> i64 {
        0
    }

    fn check_connection(&mut self) -> bool {
        true
    }

    fn is_open(&self) -> bool {
        true
    }
}

pub struct MockEnv {
    #[cfg(feature = "cluster-async")]
    pub runtime: Runtime,
    pub client: redis::cluster::ClusterClient,
    pub connection: redis::cluster::ClusterConnection<MockConnection>,
    #[cfg(feature = "cluster-async")]
    pub async_connection: redis::cluster_async::ClusterConnection<MockConnection>,
    #[allow(unused)]
    pub handler: RemoveHandler,
}

pub struct RemoveHandler(Vec<String>);

impl Drop for RemoveHandler {
    fn drop(&mut self) {
        for id in &self.0 {
            HANDLERS.write().unwrap().remove(id);
        }
    }
}

impl MockEnv {
    pub fn new(
        id: &str,
        handler: impl Fn(&[u8], u16) -> Result<(), RedisResult<Value>> + Send + Sync + 'static,
    ) -> Self {
        Self::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{id}")]),
            id,
            handler,
        )
    }

    pub fn with_client_builder(
        client_builder: ClusterClientBuilder,
        id: &str,
        handler: impl Fn(&[u8], u16) -> Result<(), RedisResult<Value>> + Send + Sync + 'static,
    ) -> Self {
        Self::with_client_builder_and_config(
            client_builder,
            redis::cluster::ClusterConfig::new(),
            id,
            handler,
        )
    }

    pub fn with_client_builder_and_config(
        client_builder: ClusterClientBuilder,
        connection_config: redis::cluster::ClusterConfig,
        id: &str,
        handler: impl Fn(&[u8], u16) -> Result<(), RedisResult<Value>> + Send + Sync + 'static,
    ) -> Self {
        #[cfg(feature = "cluster-async")]
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        let id = id.to_string();
        HANDLERS
            .write()
            .unwrap()
            .insert(id.clone(), Arc::new(move |cmd, port| handler(cmd, port)));

        let client = client_builder.build().unwrap();
        let connection = client
            .get_generic_connection_with_config(connection_config)
            .unwrap();
        #[cfg(feature = "cluster-async")]
        let async_connection = runtime
            .block_on(client.get_async_generic_connection())
            .unwrap();
        MockEnv {
            #[cfg(feature = "cluster-async")]
            runtime,
            client,
            connection,
            #[cfg(feature = "cluster-async")]
            async_connection,
            handler: RemoveHandler(vec![id]),
        }
    }
}
