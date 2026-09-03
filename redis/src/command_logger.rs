//! Opt-in, environment-driven logging of every command sent on a connection.
//!
//! This is a debugging aid in the spirit of redis' `MONITOR`, but driven entirely
//! from the client side and gated behind environment variables so it can be turned
//! on in production without code changes.
//!
//! Two independent sinks are supported, each enabled by its own environment
//! variable. Both can be active at the same time.
//!
//! * `REDIS_COMMAND_LOG_PATH` - a *directory*. When set, every connection opens its
//!   own randomly-named file inside that directory (e.g.
//!   `redis-commands-<uuid>.log`) and appends a `MONITOR`-style line per command:
//!
//!   ```text
//!   1718000000.123456 "SET" "foo" "bar"
//!   ```
//!
//! * `REDIS_COMMAND_TEE` - a `host:port` (UDP). When set, every command is also sent
//!   as a single UDP datagram of the form:
//!
//!   ```text
//!   <client uuid>:<unix time>: <command>
//!   ```
//!
//! The feature is intentionally best-effort: it never fails a real request. If a
//! file cannot be opened or a datagram cannot be sent, the error is reported once to
//! stderr and that sink is disabled for the affected connection.
//!
//! Because the data handed to [`CommandLogger::log`] is the already-encoded RESP
//! payload, a single call may contain more than one command (a pipeline or a
//! transaction); each is decoded and logged on its own line/datagram.

use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::net::{ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Environment variable naming a directory into which per-connection command logs
/// are written.
const ENV_LOG_PATH: &str = "REDIS_COMMAND_LOG_PATH";
/// Environment variable naming a `host:port` UDP endpoint to tee commands to.
const ENV_TEE: &str = "REDIS_COMMAND_TEE";
/// Port used for `REDIS_COMMAND_TEE` when the value does not contain a `:port`.
const DEFAULT_TEE_PORT: u16 = 9999;

/// Process-wide configuration, parsed once from the environment.
struct GlobalConfig {
    /// Directory to write per-connection log files into, if enabled.
    log_dir: Option<PathBuf>,
    /// Resolved UDP destination(s) for teeing, if enabled.
    tee_targets: Option<Vec<std::net::SocketAddr>>,
}

impl GlobalConfig {
    fn enabled(&self) -> bool {
        self.log_dir.is_some() || self.tee_targets.is_some()
    }
}

fn global_config() -> &'static GlobalConfig {
    static CONFIG: OnceLock<GlobalConfig> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let log_dir = std::env::var_os(ENV_LOG_PATH)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from);

        let tee_targets = std::env::var(ENV_TEE)
            .ok()
            .filter(|v| !v.is_empty())
            .and_then(|spec| match resolve_tee_targets(&spec) {
                Ok(targets) => Some(targets),
                Err(err) => {
                    eprintln!("redis: ignoring {ENV_TEE}={spec:?}: {err}");
                    None
                }
            });

        GlobalConfig {
            log_dir,
            tee_targets,
        }
    })
}

/// Resolve the `REDIS_COMMAND_TEE` value into a set of UDP socket addresses.
///
/// Accepts `host:port` or a bare `host` (in which case [`DEFAULT_TEE_PORT`] is
/// used).
fn resolve_tee_targets(spec: &str) -> std::io::Result<Vec<std::net::SocketAddr>> {
    // If the value already looks like `host:port`, resolve it directly; otherwise
    // append the default port. We can't just check for a `:` because IPv6
    // literals contain colons, but those are expected to be bracketed
    // (`[::1]:9999`) which `to_socket_addrs` already understands.
    let resolved: Vec<_> = match spec.to_socket_addrs() {
        Ok(addrs) => addrs.collect(),
        Err(_) => (spec, DEFAULT_TEE_PORT).to_socket_addrs()?.collect(),
    };
    if resolved.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "host did not resolve to any address",
        ));
    }
    Ok(resolved)
}

/// Per-connection command logger. Cheap to clone via `Arc`; shared by all clones of
/// a multiplexed connection so they share one file and one client uuid.
pub(crate) struct CommandLogger {
    /// Stable per-connection identifier, also embedded in the log file name.
    uuid: String,
    /// Append target for `REDIS_COMMAND_LOG_PATH`, behind a mutex to keep lines from
    /// interleaving across threads.
    file: Option<Mutex<File>>,
    /// UDP socket + destination(s) for `REDIS_COMMAND_TEE`.
    tee: Option<(UdpSocket, Vec<std::net::SocketAddr>)>,
}

impl CommandLogger {
    /// Build a logger for a freshly-established connection, or `None` if neither
    /// environment variable is set (the common case, which stays zero-cost).
    pub(crate) fn for_new_connection() -> Option<Self> {
        let config = global_config();
        if !config.enabled() {
            return None;
        }

        let uuid = format_uuid(random_u128());

        let file = config.log_dir.as_ref().and_then(|dir| {
            let path = dir.join(format!("redis-commands-{uuid}.log"));
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(file) => {
                    eprintln!("redis: logging commands for connection {uuid} to {path:?}");
                    Some(Mutex::new(file))
                }
                Err(err) => {
                    eprintln!("redis: could not open command log {path:?}: {err}");
                    None
                }
            }
        });

        let tee =
            config
                .tee_targets
                .as_ref()
                .and_then(|targets| match UdpSocket::bind(("0.0.0.0", 0)) {
                    Ok(socket) => {
                        eprintln!("redis: teeing commands for connection {uuid} to {targets:?}");
                        Some((socket, targets.clone()))
                    }
                    Err(err) => {
                        eprintln!("redis: could not open command tee socket: {err}");
                        None
                    }
                });

        // If both sinks failed to initialize there is nothing to log to.
        if file.is_none() && tee.is_none() {
            return None;
        }

        Some(CommandLogger { uuid, file, tee })
    }

    /// Log every command contained in an already-encoded RESP `packed` payload.
    ///
    /// Best-effort: any I/O error disables nothing permanently but is otherwise
    /// swallowed so that logging never breaks a real request.
    pub(crate) fn log(&self, packed: &[u8]) {
        let timestamp = unix_timestamp();
        for command in decode_commands(packed) {
            if let Some(file) = &self.file {
                if let Ok(mut file) = file.lock() {
                    // Format: `<unix time> <quoted args>`, MONITOR-style.
                    let _ = writeln!(file, "{timestamp} {command}");
                }
            }
            if let Some((socket, targets)) = &self.tee {
                // Format: `<client uuid>:<unix time>: <command>`.
                let datagram = format!("{}:{timestamp}: {command}", self.uuid);
                for target in targets {
                    if socket.send_to(datagram.as_bytes(), target).is_ok() {
                        break;
                    }
                }
            }
        }
    }
}

/// Decode a RESP payload of one or more commands into MONITOR-style, quoted strings.
///
/// Each returned element corresponds to a single command, e.g. `"SET" "foo" "bar"`.
/// If the payload cannot be fully decoded (which should not happen for data this
/// crate produced) whatever was decoded so far is returned plus a marker line.
fn decode_commands(packed: &[u8]) -> Vec<String> {
    let mut commands = Vec::new();
    let mut pos = 0;
    while pos < packed.len() {
        match decode_one_command(packed, &mut pos) {
            Some(line) => commands.push(line),
            None => {
                commands.push(format!("<unparsed {} bytes>", packed.len() - pos));
                break;
            }
        }
    }
    commands
}

/// Decode a single `*<argc>\r\n($<len>\r\n<bytes>\r\n)*` command starting at `*pos`,
/// advancing `pos` past it. Returns the quoted, space-joined arguments.
fn decode_one_command(buf: &[u8], pos: &mut usize) -> Option<String> {
    let argc = read_count(buf, pos, b'*')?;
    let mut out = String::new();
    for i in 0..argc {
        let len = read_count(buf, pos, b'$')?;
        let end = pos.checked_add(len)?;
        let arg = buf.get(*pos..end)?;
        *pos = end;
        // consume trailing CRLF
        if buf.get(*pos..*pos + 2)? != b"\r\n" {
            return None;
        }
        *pos += 2;
        if i > 0 {
            out.push(' ');
        }
        quote_arg(arg, &mut out);
    }
    Some(out)
}

/// Read a `<prefix><number>\r\n` header, advancing `pos` past it.
fn read_count(buf: &[u8], pos: &mut usize, prefix: u8) -> Option<usize> {
    if *buf.get(*pos)? != prefix {
        return None;
    }
    *pos += 1;
    let start = *pos;
    while *buf.get(*pos)? != b'\r' {
        *pos += 1;
    }
    let digits = std::str::from_utf8(&buf[start..*pos]).ok()?;
    let value = digits.parse::<usize>().ok()?;
    // consume CRLF
    if buf.get(*pos..*pos + 2)? != b"\r\n" {
        return None;
    }
    *pos += 2;
    Some(value)
}

/// Append `arg` to `out` as a double-quoted, escaped string, matching the style of
/// redis' `MONITOR` output.
fn quote_arg(arg: &[u8], out: &mut String) {
    out.push('"');
    for &b in arg {
        match b {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7e => out.push(b as char),
            _ => {
                let _ = write!(out, "\\x{b:02x}");
            }
        }
    }
    out.push('"');
}

/// Current time as a `seconds.microseconds` unix timestamp string.
fn unix_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => format!("{}.{:06}", d.as_secs(), d.subsec_micros()),
        Err(_) => "0.000000".to_string(),
    }
}

/// Generate a 128-bit value with enough entropy to uniquely tag a connection,
/// without taking on a dependency on `rand` (which is only an optional feature).
fn random_u128() -> u128 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = u128::from(std::process::id());
    // The address of a stack local varies between calls and across processes,
    // adding entropy that survives identical wall-clock readings.
    let stack_addr = (&count as *const u64 as usize) as u128;

    let mut seed = [0u8; 56];
    seed[0..16].copy_from_slice(&nanos.to_le_bytes());
    seed[16..24].copy_from_slice(&count.to_le_bytes());
    seed[24..40].copy_from_slice(&pid.to_le_bytes());
    seed[40..56].copy_from_slice(&stack_addr.to_le_bytes());
    xxhash_rust::xxh3::xxh3_128(&seed)
}

/// Render a 128-bit value as a canonical `8-4-4-4-12` UUID-style string.
fn format_uuid(v: u128) -> String {
    let b = v.to_be_bytes();
    let mut s = String::with_capacity(36);
    for (i, byte) in b.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        let _ = write!(s, "{byte:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_single_command() {
        let packed = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        assert_eq!(decode_commands(packed), vec![r#""SET" "foo" "bar""#]);
    }

    #[test]
    fn decodes_pipeline_of_commands() {
        let packed = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nx\r\n";
        assert_eq!(
            decode_commands(packed),
            vec![r#""PING""#.to_string(), r#""GET" "x""#.to_string()]
        );
    }

    #[test]
    fn escapes_binary_arguments() {
        let packed = b"*2\r\n$3\r\nSET\r\n$2\r\n\x00\xff\r\n";
        assert_eq!(decode_commands(packed), vec![r#""SET" "\x00\xff""#]);
    }

    #[test]
    fn uuid_has_canonical_shape() {
        let uuid = format_uuid(0x0123456789abcdef0123456789abcdef);
        assert_eq!(uuid, "01234567-89ab-cdef-0123-456789abcdef");
    }

    #[test]
    fn random_ids_differ() {
        assert_ne!(random_u128(), random_u128());
    }
}
