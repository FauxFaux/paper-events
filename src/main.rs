use anyhow::{bail, Result};
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use itertools::Itertools;
use rand::Rng;
use std::collections::HashMap;
use std::env;
use std::time::Instant;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

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
    let (stream, _) = listener.accept().await?;
    let mut stream = FramedWrite::new(stream, LengthDelimitedCodec::new());

    let mut buf = BytesMut::with_capacity(4096);
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

        bincode::encode_into_writer(msg, Writer { buf: &mut buf }, config)?;
        stream.send(buf.split().freeze()).await?;
    }
}

async fn client() -> Result<()> {
    let client = TcpStream::connect("localhost:9966").await?;
    let mut client = FramedRead::new(client, LengthDelimitedCodec::new());

    let config = bincode::config::legacy();

    let mut hash = HashMap::<u64, u64>::with_capacity(100_000_000);

    let mut counter = 0usize;
    let start = Instant::now();
    while let Some(msg) = client.next().await {
        let msg = msg?;

        let msg: Msg = bincode::decode_from_slice(&msg, config)?.0;
        *hash.entry(msg.id).or_insert(0) += 1;

        counter += 1;

        if counter % 2usize.pow(22) == 0 {
            let elapsed = start.elapsed();
            let msgs_per_second = counter as f64 / elapsed.as_secs_f64() / 1e6;
            println!("{msgs_per_second:.3}M/s");
        }

        if counter % 2usize.pow(28) == 0 {
            let mut sample = hash.iter().take(10_000).map(|(_, v)| *v).collect_vec();
            sample.sort_unstable();
            let median = sample[sample.len() / 2];

            let before = hash.len();

            hash.retain(|_, v| *v >= median);

            let top = hash
                .iter()
                .filter(|(_, v)| **v > median)
                .sorted_by_key(|(_, views)| std::cmp::Reverse(*views))
                .take(10)
                .collect_vec();
            let elapsed = start.elapsed();
            let msgs_per_second = counter as f64 / elapsed.as_secs_f64() / 1e6;
            println!("Received {counter} messages, {} ids tracked, {} rejected for being below {median}, {msgs_per_second:.2}M/s, top k: {top:?}", hash.len(), before - hash.len());
        }
    }
    Ok(())
}

struct Writer<'b> {
    buf: &'b mut BytesMut,
}

impl<'b> bincode::enc::write::Writer for Writer<'b> {
    fn write(&mut self, bytes: &[u8]) -> Result<(), bincode::error::EncodeError> {
        self.buf.extend_from_slice(bytes);
        Ok(())
    }
}
