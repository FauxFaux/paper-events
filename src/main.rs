use anyhow::{bail, Result};
use itertools::Itertools;
use rand::Rng;
use std::collections::HashMap;
use std::env;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, bincode::Encode, bincode::Decode)]
struct Msg {
    id: u64,
    start: u64,
    end: u64,
    duration: u64,
    user_id: u128,
}

#[tokio::main]
async fn main() -> Result<()> {
    match env::args().nth(1).as_deref() {
        Some("serve") => serve().await,
        Some("client") => client().await,
        _ => bail!("Usage: {} [serve|client]", env::args().next().unwrap()),
    }
}

async fn serve() -> Result<()> {
    let mut rand = rand::thread_rng();
    let listener = TcpListener::bind("localhost:9966").await?;
    let (mut stream, _) = listener.accept().await?;
    let mut buf = [0u8; 1024];
    let config = bincode::config::legacy();

    loop {
        let the_u64 = rand.gen();
        let msg = Msg {
            id: (rand.gen::<u64>() as f64 * rand.gen::<f64>().powf(3.)) as u64,
            start: the_u64,
            end: the_u64,
            duration: the_u64,
            user_id: rand.gen(),
        };
        let len = bincode::encode_into_slice(msg, &mut buf[4..], config)?;
        let len = len + 4;
        buf[..4].copy_from_slice(&(len as u32).to_le_bytes());
        stream.write_all(&buf[..len]).await?;
    }
}

async fn client() -> Result<()> {
    let mut client = BufReader::new(TcpStream::connect("localhost:9966").await?);
    let mut buf = [0u8; 1024];
    let config = bincode::config::legacy();

    let mut hash = HashMap::<u64, u64>::with_capacity(100_000_000);

    let mut counter = 0usize;
    let start = Instant::now();
    loop {
        let mut len_bytes = [0u8; 4];
        client.read_exact(&mut len_bytes).await?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        let len = len - 4;
        client.read_exact(&mut buf[..len]).await?;

        let msg: Msg = bincode::decode_from_slice(&buf, config)?.0;
        *hash.entry(msg.id).or_insert(0) += 1;

        counter += 1;

        if counter % 2usize.pow(23) == 0 {
            let top = hash
                .iter()
                .sorted_by_key(|(_, views)| std::cmp::Reverse(*views))
                .take(10)
                .collect_vec();
            let elapsed = start.elapsed();
            let msgs_per_second = counter as f64 / elapsed.as_secs_f64() / 1e6;
            println!("Received {counter} messages, {} ids tracked, {msgs_per_second:.2}M/s, top k: {top:?}", hash.len());
        }
    }
}
