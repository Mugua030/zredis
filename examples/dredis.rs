use std::{io, net::SocketAddr};

use anyhow::Result;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tracing::{info, warn};

const BUF_SIZE: usize = 2048;
#[tokio::main]
async fn main() -> Result<()> {
    //logging
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:7379";
    let listener = TcpListener::bind(addr).await?;
    info!("dredis: listening on {}", addr);
    // Get data from the socket
    loop {
        let (stream, saddr) = listener.accept().await?;
        info!("Accept connection from: {}", saddr);

        tokio::spawn(async move {
            if let Err(e) = process_conn(stream, saddr).await {
                warn!("Error process connection: {}", e);
            }
        });
    }
}

async fn process_conn(mut stream: TcpStream, saddr: SocketAddr) -> Result<()> {
    loop {
        stream.readable().await?;
        let mut buf: Vec<u8> = Vec::with_capacity(BUF_SIZE);
        match stream.try_read_buf(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                info!("read {} bytes", n);
                //let buf_utfu16: Vec<u16> = buf.iter().map(|&x| x as u16).collect();
                //let line = String::from_utf16_lossy(&buf_utfu16[..]);
                let line = String::from_utf8_lossy(&buf);
                info!("{:?}", line);
                stream.write_all(b"+OK\r\n").await?;
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    warn!("Connection {} closed", saddr);
    Ok(())
}
