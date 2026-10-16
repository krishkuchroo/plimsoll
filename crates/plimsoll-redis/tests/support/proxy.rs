//! A tiny fault-injecting TCP proxy for tests, in the spirit of Toxiproxy.
//!
//! Sits between `RedisStore` and Redis. The mode can be switched at any time
//! and applies to traffic from the client towards Redis:
//!
//! - `Pass`: forward unchanged.
//! - `Latency(d)`: wait `d` before forwarding each chunk.
//! - `Blackhole`: accept and read, but never forward (requests hang).
//! - `Refuse`: close new connections immediately.
//!
//! `cut()` drops every open connection, like a network blip or a Redis
//! restart as seen from the client.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Pass,
    Latency(Duration),
    Blackhole,
    Refuse,
}

pub struct FaultProxy {
    addr: SocketAddr,
    mode: Arc<Mutex<Mode>>,
    cut: watch::Sender<u64>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl FaultProxy {
    /// Start a proxy on a random local port forwarding to `upstream`
    /// (`host:port`).
    pub async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mode = Arc::new(Mutex::new(Mode::Pass));
        let (cut, _) = watch::channel(0u64);

        let accept_task = {
            let (mode, cut) = (Arc::clone(&mode), cut.clone());
            tokio::spawn(async move {
                loop {
                    let Ok((client, _)) = listener.accept().await else {
                        return;
                    };
                    if *mode.lock().unwrap() == Mode::Refuse {
                        drop(client);
                        continue;
                    }
                    let Ok(server) = TcpStream::connect(&upstream).await else {
                        continue;
                    };
                    tokio::spawn(relay(client, server, Arc::clone(&mode), cut.subscribe()));
                }
            })
        };
        Self {
            addr,
            mode,
            cut,
            accept_task,
        }
    }

    /// URL that reaches Redis through this proxy.
    pub fn url(&self) -> String {
        format!("redis://{}/", self.addr)
    }

    pub fn set(&self, mode: Mode) {
        *self.mode.lock().unwrap() = mode;
    }

    /// Drop every open connection.
    pub fn cut(&self) {
        self.cut.send_modify(|n| *n += 1);
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.accept_task.abort();
        self.cut();
    }
}

async fn relay(
    client: TcpStream,
    server: TcpStream,
    mode: Arc<Mutex<Mode>>,
    mut cut: watch::Receiver<u64>,
) {
    let (mut c_read, mut c_write) = client.into_split();
    let (mut s_read, mut s_write) = server.into_split();

    let upstream = async {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = match c_read.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            let current = *mode.lock().unwrap();
            match current {
                Mode::Blackhole => continue,
                Mode::Latency(d) => tokio::time::sleep(d).await,
                Mode::Pass | Mode::Refuse => {}
            }
            if s_write.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    };
    let downstream = async {
        let _ = tokio::io::copy(&mut s_read, &mut c_write).await;
    };

    tokio::select! {
        _ = upstream => {}
        _ = downstream => {}
        _ = cut.changed() => {}
    }
    // Dropping the halves closes both sockets.
}

/// `host:port` of the real Redis behind `url` (`redis://[user:pass@]host[:port][/db]`).
pub fn upstream_of(url: &str) -> String {
    let rest = url.split("://").nth(1).unwrap_or(url);
    let host_port = rest.split('/').next().unwrap();
    let host_port = host_port.rsplit('@').next().unwrap();
    if host_port.contains(':') {
        host_port.to_owned()
    } else {
        format!("{host_port}:6379")
    }
}
