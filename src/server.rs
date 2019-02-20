use std::sync::Arc;
use std::io::Cursor;

use clap::App;

use hex::encode;

use tokio::io::{read_exact, write_all};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use log::{info, debug, trace};

use kitap::mapper::{Mapper, MapperReply};
use kitap::utils::{SharedBuffer, BoxedFuture};
use kitap::utils::{create_base_app, setup_logging};
use kitap::messages::{MessageType, PlaceMessage};
use kitap::messages::{MSG_HEADER_LEN};

type VecVecMapper = Mapper<Vec<u8>, Vec<u8>>;

const NOTFOUND: [u8; 9] = [5, 0, 0, 0, 78, 84, 70, 78, 68];
const ERROR: [u8; 9] = [5, 0, 0, 0, 69, 82, 82, 79, 82];

fn process_fetch(cloned_mapper: Arc<VecVecMapper>, buf: Vec<u8>, wx: tokio::io::WriteHalf<TcpStream>) -> BoxedFuture<(), String> {
    let key = buf;
    info!("Received fetch message for key: {}", encode(&key));
    Box::new(cloned_mapper.get(key)
        .and_then(|reply| {
            debug!("Got reply from mapper {:?}", reply);
            let w = match reply {
                MapperReply::Data(r) => {
                    let mut v = Vec::with_capacity(4 + r.data.len());
                    v.write_u32::<LittleEndian>(r.data.len() as u32).unwrap();
                    v.extend(r.data.as_ref());
                    trace!("Retrieved data {}", encode(&v));
                    SharedBuffer::new(Arc::new(v))
                },
                MapperReply::NotFound => SharedBuffer::new(Arc::new(NOTFOUND.to_vec())),
                _ => SharedBuffer::new(Arc::new(ERROR.to_vec())),
            };
            write_all(wx, w)
                .map(|_| info!("Sent response back to client"))
                .map_err(|_| "Could not sent response".to_string())
        }))
}

fn process_place(cloned_mapper: Arc<VecVecMapper>, buf: Vec<u8>, wx: tokio::io::WriteHalf<TcpStream>, rx: tokio::io::ReadHalf<TcpStream>) -> BoxedFuture<(), String> {
    trace!("buf: {:?}, len: {}", encode(&buf), buf.len());
    let msg = match PlaceMessage::try_from(buf) {
        Ok(m) => m,
        Err(s) => return Box::new(future::err(s)),
    };
    info!("Received place message for key: {}", encode(&msg.hash));
    trace!("datasize {}", msg.datasize);
    let mut data = Vec::with_capacity(msg.datasize);
    data.resize(msg.datasize, 0);
    Box::new(
        read_exact(rx, data)
        .map_err(|_| "Could not read data".to_string())
        .and_then(move |(_, data)| cloned_mapper.set(msg.hash, data))
        .and_then(|reply| {
            debug!("Got reply from mapper {:?}", reply);
            let w = match reply {
                MapperReply::Ok => SharedBuffer::new(Arc::new(String::from("ITSOK").into_bytes())),
                _ => SharedBuffer::new(Arc::new(String::from("ERROR").into_bytes())),
            };
            write_all(wx, w)
                .map(|_| info!("Sent response back to client"))
                .map_err(|_| "Could not sent response".to_string())
        }))
}

fn create_parser() -> App<'static, 'static> {
    create_base_app("kitapd")
}

fn main() {
    let matches = create_parser().get_matches();
    let verbosity = matches.occurrences_of("verbose");
    let logfile = matches.value_of("logfile");

    setup_logging(verbosity, logfile).expect("Logging could not be setup");

    info!("Starting up kitapd!");

    let mut mapper = Mapper::new();
    // Bind the server's socket.
    let addr = "127.0.0.1:12345".parse().unwrap();
    let listener = TcpListener::bind(&addr).expect("unable to bind TCP listener");

    tokio::run(future::lazy(|| {

        let hashmap_thread = mapper.receive().unwrap();
        let shared_mapper = Arc::new(mapper);
        tokio::spawn(hashmap_thread);
        debug!("Mapper spawned");

        // Pull out a stream of sockets for incoming connections
        let server = listener
            .incoming()
            .map_err(|e| debug!("accept failed = {:?}", e))
            .for_each(move |sock| {
                info!("Connected with {}", sock.peer_addr().unwrap());
                let cloned_mapper = shared_mapper.clone();
                let (rx, wx) = sock.split();
                let mut buf = Vec::with_capacity(MSG_HEADER_LEN);
                buf.resize(MSG_HEADER_LEN, 0);
                let task = read_exact(rx, buf)
                    .map_err(|_| "something bad happened when reading the header".to_string())
                    .and_then(move |(rx, b)| {
                        let mut cursor = Cursor::new(b);
                        // XXX should these unwraps be handled? The only way they can fail is if
                        // cursor has less bytes than needed, but then this closure would not
                        // execute, as the previous read_exact would not have returned a value
                        let req_type = cursor.read_u16::<LittleEndian>().unwrap().into();
                        let length = cursor.read_u32::<LittleEndian>().unwrap() as usize;
                        let mut buf = Vec::with_capacity(length);
                        buf.resize(length, 0);
                        debug!("req_type: {:?}, length: {}", req_type, length);
                        read_exact(rx, buf)
                            .map(move |(rx, buf)| (rx, buf, req_type))
                            .map_err(|_| "something bad happened when reading the body".to_string())
                    })
                    .and_then(|(rx, b, req_type)| {
                        let r = match req_type {
                            MessageType::Place => {
                                process_place(cloned_mapper, b, wx, rx)
                            },
                            MessageType::Fetch => {
                                process_fetch(cloned_mapper, b, wx)
                            }
                            _ => Box::new(future::err("Unkown message type".to_string()))
                        };
                        r
                    })
                    .map_err(|e| info!("{}", e))
                    .map(|_| info!("request served"));
                tokio::spawn(task);
                Ok(())
            });

        tokio::spawn(server);
        debug!("Server spawned");
        future::ok(())
    }));
}
