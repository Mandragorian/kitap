use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::fs::File;
use std::io::{Bytes, Read, BufReader};

use chrono;
use fern;
use log;

use itertools::Itertools;
use itertools::structs::IntoChunks;

use futures::future::Future;

use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::prelude::AsyncRead;

use clap::{App, Arg};

use crate::hash::{KitapHasher, KitapHash};

pub type BoxedFuture<T, E> = Box<dyn Future<Item = T, Error = E> + Send>;

#[derive(Debug)]
pub struct SharedBuffer<T: AsRef<[u8]>>(Arc<T>);

impl<T> SharedBuffer<T>
where
    T: AsRef<[u8]>,
{
    pub fn new(t: Arc<T>) -> SharedBuffer<T> {
        SharedBuffer(t)
    }
}

impl<T> AsRef<[u8]> for SharedBuffer<T>
where
    T: AsRef<[u8]>,
{
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref().as_ref()
    }
}

pub fn connect(
    addr: SocketAddr,
) -> impl Future<Item = (ReadHalf<TcpStream>, WriteHalf<TcpStream>), Error = ()> {
    TcpStream::connect(&addr)
        .map_err(|e| eprintln!("could not connect: {}", e))
        .map(|s| s.split())
}

pub fn setup_logging(verbosity: u64, logfile: Option<&str>) -> Result<(), fern::InitError> {
    let mut base_config = fern::Dispatch::new();

    base_config = match verbosity {
        0 => {
            base_config
                .level(log::LevelFilter::Info)
                .level_for("tokio_reactor", log::LevelFilter::Warn)
                .level_for("kitap::mapper", log::LevelFilter::Warn)
        }
        1 => base_config
            .level(log::LevelFilter::Debug)
            .level_for("tokio_reactor", log::LevelFilter::Info)
            .level_for("kitap::mapper", log::LevelFilter::Info),
        2 => base_config.level(log::LevelFilter::Debug),
        _3_or_more => base_config
            .level(log::LevelFilter::Trace)
            .level_for("tokio_reactor", log::LevelFilter::Debug)
            .level_for("mio", log::LevelFilter::Debug)
            .level_for("tokio_threadpool", log::LevelFilter::Debug)
            .level_for("kitap::mapper", log::LevelFilter::Debug),
    };

    let config = fern::Dispatch::new()
        .format(|out, message, record| {
            // special format for debug messages coming from our own crate.
            out.finish(format_args!(
                "{}[{}][{}:{}+{}] {}",
                chrono::Local::now().format("[%Y-%m-%d %H:%M:%S]"),
                record.level(),
                record.target(),
                record.file().unwrap(),
                record.line().unwrap(),
                message
            ))
        })
        .chain(io::stdout());

    match logfile {
        Some(logfile) => {
            let fileconfig = config.chain(fern::log_file(logfile)?);
            base_config.chain(fileconfig)
        },
        None => {
            base_config.chain(config)
        }
    }.apply()?;

    Ok(())
}

pub fn create_base_app(name: &str) -> App<'static, 'static> {
    App::new(name)
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("--verbose")
                .multiple(true)
                .help("Increases logging verbosity each use for up to 3 times"),
        )
        .arg(
            Arg::with_name("logfile")
                .long("--logfile")
                .help("Output logs to a file")
                .takes_value(true),
        ).arg(
            Arg::with_name("host")
                .long("--host")
                .help("The host to connect to")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("port")
                .long("--port")
                .short("-p")
                .help("The port to connect to")
                .takes_value(true)
                .required(true),
        )

}

pub fn file_chunks(filename: &str) -> Result<IntoChunks<Bytes<BufReader<File>>>, String> {
    let f = File::open(filename).or(Err(format!("Could not open {}", filename)))?;
    Ok(BufReader::new(f).bytes().chunks(1024))
}

pub fn hash_file(filename: &str) -> Result<KitapHash, String> {
    let mut hasher = KitapHasher::new();
    let reader = file_chunks(filename)?;//?.bytes().chunks(1024);
    for chunk in &reader {
        let v: Result<Vec<u8>, std::io::Error>  = chunk.collect();
        let v = v.or(Err(format!("Could not read from {}", filename)))?;
        hasher.input(v);
    }
    Ok(hasher.result())
}
