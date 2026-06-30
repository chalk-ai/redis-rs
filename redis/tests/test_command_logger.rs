//! End-to-end tests for the environment-driven command logger
//! (`REDIS_COMMAND_LOG_PATH` / `REDIS_COMMAND_TEE`).
//!
//! These drive a real synchronous [`redis::Connection`] against a tiny in-process
//! fake server that answers every command with `+OK`, and assert that commands are
//! mirrored to the configured file and UDP sinks.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::thread;
use std::time::Duration;

/// Read a single RESP `*<argc>\r\n($<len>\r\n<bytes>\r\n)*` command from `reader`.
/// Returns `None` at end of stream.
fn read_command(reader: &mut BufReader<TcpStream>) -> Option<Vec<Vec<u8>>> {
    let argc = read_count(reader, b'*')?;
    let mut args = Vec::with_capacity(argc);
    for _ in 0..argc {
        let len = read_count(reader, b'$')?;
        let mut buf = vec![0u8; len + 2]; // payload + CRLF
        reader.read_exact(&mut buf).ok()?;
        buf.truncate(len);
        args.push(buf);
    }
    Some(args)
}

/// Read a `<prefix><number>\r\n` header line.
fn read_count(reader: &mut BufReader<TcpStream>, prefix: u8) -> Option<usize> {
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line).ok()? == 0 {
        return None; // EOF
    }
    assert_eq!(line.first(), Some(&prefix), "unexpected RESP framing");
    let digits = std::str::from_utf8(&line[1..]).ok()?.trim();
    digits.parse().ok()
}

/// Spawn a fake redis server that answers every command with `+OK`. Returns the
/// address it is listening on.
fn spawn_fake_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            while read_command(&mut reader).is_some() {
                if writer.write_all(b"+OK\r\n").is_err() {
                    break;
                }
            }
        }
    });
    addr
}

#[test]
fn logs_commands_to_file_and_tees_over_udp() {
    // A UDP listener for REDIS_COMMAND_TEE.
    let udp = UdpSocket::bind(("127.0.0.1", 0)).unwrap();
    udp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let tee_addr = udp.local_addr().unwrap();

    // A directory for REDIS_COMMAND_LOG_PATH.
    let log_dir = tempfile::tempdir().unwrap();

    // SAFETY: set before any connection is opened in this single-threaded test, so
    // the logger's process-wide config is initialized from these values.
    unsafe {
        std::env::set_var("REDIS_COMMAND_LOG_PATH", log_dir.path());
        std::env::set_var("REDIS_COMMAND_TEE", tee_addr.to_string());
    }

    let server_addr = spawn_fake_server();

    let client = redis::Client::open((server_addr.ip().to_string(), server_addr.port())).unwrap();
    let mut con = client.get_connection().unwrap();

    redis::cmd("SET")
        .arg("foo")
        .arg("bar")
        .exec(&mut con)
        .unwrap();

    let expected = r#""SET" "foo" "bar""#;

    // The UDP tee datagram should be `<uuid>:<unix time>: <command>`.
    let mut buf = [0u8; 1024];
    let (n, _) = udp.recv_from(&mut buf).expect("expected a tee datagram");
    let datagram = std::str::from_utf8(&buf[..n]).unwrap();
    assert!(
        datagram.ends_with(&format!(": {expected}")),
        "datagram {datagram:?} did not end with the expected command",
    );
    // Sanity check the `<uuid>:<unix time>:` prefix.
    let prefix = datagram.split(": ").next().unwrap();
    let (uuid, ts) = prefix.split_once(':').expect("uuid:timestamp prefix");
    assert_eq!(uuid.len(), 36, "uuid should be canonical length: {uuid:?}");
    assert!(ts.contains('.'), "timestamp should be sub-second: {ts:?}");

    // Exactly one per-connection log file should have been created.
    let mut log_files: Vec<_> = std::fs::read_dir(log_dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert_eq!(
        log_files.len(),
        1,
        "expected one log file, got {log_files:?}"
    );
    let log_path = log_files.pop().unwrap();
    assert!(
        log_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("redis-commands-"),
    );

    let mut contents = String::new();
    std::fs::File::open(&log_path)
        .unwrap()
        .read_to_string(&mut contents)
        .unwrap();
    // The CLIENT SETINFO setup commands run before logging is enabled, so the file
    // should contain only the user's command.
    assert!(
        contents.contains(expected),
        "log file did not contain command, got: {contents:?}",
    );
    assert!(
        !contents.contains("SETINFO"),
        "setup commands should not be logged, got: {contents:?}",
    );
}
