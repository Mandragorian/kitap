use std::io::Cursor;
use std::net::SocketAddr;
use std::fs::File;
use std::str;

use hex::{decode, encode};

use clap::{App, Arg, ArgMatches, SubCommand};

use tokio::io::{read_exact, write_all};
use tokio::prelude::*;

use byteorder::{LittleEndian, ReadBytesExt};

use kitap::hash::HASH_SIZE;
use kitap::messages::{FetchMessage, Message, PlaceMessage};
use kitap::utils::{file_chunks, hash_file, connect, create_base_app, BoxedFuture};

fn create_parser() -> App<'static, 'static> {
    create_base_app("kitap")
        .version("0.1")
        .author("mandragore")
        .about("RustDHT client")
        .subcommand(
            SubCommand::with_name("fetch")
                .about("fethces a hash")
                .arg(Arg::with_name("hash").min_values(1).required(true)),
        )
        .subcommand(
            SubCommand::with_name("place")
                .about("places a hash")
                .arg(Arg::with_name("filename").required(true))
        )
}

type DHTJob = BoxedFuture<(), ()>;

fn fetch(addr: SocketAddr, matches: &ArgMatches) -> Result<DHTJob, String> {
    let hashes: Result<Vec<Vec<u8>>, _> = matches
        .values_of("hash")
        .unwrap()
        .map(|x| {
            let v = decode(x.to_string().into_bytes())
                .or(Err("Invalid hex value as hash"))?;
            if v.len() != HASH_SIZE {
                return Err(String::from("Hash length is wrong"));
            };
            Ok(v)
        })
        .collect();
    let hashes = hashes?;
    let client = connect(addr)
        .and_then(|(rx, wx)| {
            let msg = FetchMessage::new(hashes);
            let string = msg.into_bytes();
            let w = write_all(wx, string)
                .map(|(wx, _buf)| (rx, wx))
                .map_err(|e| eprintln!("failed to send bytes {}", e));
            w
        })
        .and_then(|(rx, _wx)| {
            let mut buf = Vec::with_capacity(4);
            buf.resize(4, 0);
            let r = read_exact(rx, buf)
                .map_err(|_| eprintln!("failed to receive bytes"))
                .and_then(|(rx, buf)| {
                    let mut cursor = Cursor::new(buf);
                    let length = cursor.read_u32::<LittleEndian>().unwrap() as usize;
                    let mut buf = Vec::with_capacity(length);
                    buf.resize(length, 0);
                    read_exact(rx, buf)
                        .map_err(|e| eprintln!("could not read response {}", e))
                        .map(|(_, buf)| println!("{}", String::from_utf8(buf).unwrap()))
                });
            tokio::spawn(r)
        });
    Ok(Box::new(client))
}

fn place(addr: SocketAddr, matches: &ArgMatches) -> Result<DHTJob, String> {
    let filename = matches.value_of("filename").unwrap();
    let f = File::open(filename).or(Err(format!("Could not open {}", filename)))?;
    let hash = hash_file(filename)?;
    println!("{}", encode(&hash));

    let datasize = f.metadata().unwrap().len() as usize;
    let data = file_chunks(filename)?;

    let client = connect(addr)
        .and_then(move |(rx, wx)| {
            let msg = PlaceMessage::new(hash.to_vec(), datasize);
            let mut vec = vec![msg.into_bytes()];
            vec.extend(data.into_iter().map(|c| {
                let v: Result<Vec<u8>, _> = c.collect();
                let v = v.unwrap();
                v
            }));
            stream::iter_ok(vec).
                fold((rx, wx), |(reader, writer) , buf| {
                    write_all(writer, buf)
                        .map(|(wx, _buf)| (reader, wx))
                        .map_err(|e| eprintln!("failed to send bytes {}", e))
                })
        })
        .and_then(|(rx, _wx)| {
            let mut buf = Vec::with_capacity(5);
            buf.resize(5, 0);
            read_exact(rx, buf)
                .map(|(_, t)| println!("{}", str::from_utf8(&t).unwrap()))
                .map_err(|e| eprintln!("failed to receive bytes {}", e))
        });
    Ok(Box::new(client))
}

fn main() -> Result<(), String> {
    let matches = create_parser().get_matches();

    let host = matches
        .value_of("host")
        .expect("Host address not specified");
    let port = matches.value_of("port").expect("Port not specified");
    let addr = format!("{}:{}", host, port).parse().or(Err("Asd"))?;

    let thread = match matches.subcommand() {
        ("fetch", Some(submatches)) => fetch(addr, submatches)?,
        ("place", Some(submatches)) => place(addr, submatches)?,
        _ => Box::new(future::err(()))
    };
    tokio::run(thread);
    Ok(())
}
