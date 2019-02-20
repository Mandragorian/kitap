use std::collections::HashMap;
use std::sync::Arc;

use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc;
use futures::sync::mpsc::{Receiver, Sender};
use futures::Future;

use log::{debug, info};

#[derive(Debug)]
pub struct DataContents<T>
{
    pub data: Arc<T>,
}

#[derive(Debug)]
pub enum MapperReply<T>
{
    Data(DataContents<T>),
    Ok,
    NotFound,
}

#[derive(Debug)]
struct FetchContents<K>
where
    K: std::hash::Hash + std::cmp::Eq,
{
    pub key: K,
}

#[derive(Debug)]
struct PlaceContents<K, T>
where
    K: std::hash::Hash + std::cmp::Eq,
{
    pub key: K,
    pub data: T,
}

#[derive(Debug)]
enum Contents<K, T>
where
    K: std::hash::Hash + std::cmp::Eq,
{
    Fetch(FetchContents<K>),
    Place(PlaceContents<K, T>),
}

#[derive(Debug)]
struct RequestMessage<K, T>
where
    K: std::hash::Hash + std::cmp::Eq,
{
    pub contents: Contents<K, T>,
    pub snd: Sender<MapperReply<T>>,
}

#[derive(Debug)]
pub struct Mapper<K, T>
where
    K: std::hash::Hash + std::cmp::Eq,
{
    map: Option<HashMap<K, Arc<T>>>,
    sender: Sender<RequestMessage<K, T>>,
    receiver: Option<Receiver<RequestMessage<K, T>>>,
}

impl<K, T> Mapper<K, T>
where
    K: std::hash::Hash + std::cmp::Eq,
{
    pub fn new() -> Mapper<K, T> {
        let map = Some(HashMap::new());
        let (sender, receiver) = mpsc::channel::<RequestMessage<K, T>>(1);
        let receiver = Some(receiver);
        Mapper {
            map,
            sender,
            receiver,
        }
    }

    pub fn owned_insert(&mut self, k: K, t: T) -> Result<(), String> {
        match self.map {
            Some(ref mut m) => {
                m.insert(k, Arc::new(t));
                Ok(())
            },
            None => Err(String::from("The mapper no longer owns its map"))
        }
    }

    pub fn owned_get(&self, k: &K) -> Result<Option<&Arc<T>>, String> {
        match self.map {
            Some(ref m) => {
                Ok(m.get(k))
            },
            None => Err(String::from("The mapper no longer owns its map"))
        }
    }

    fn send_request(&self, contents: Contents<K, T>) -> impl Future< Item = MapperReply<T>, Error = String> {
        let (snd, rcv) = mpsc::channel::<MapperReply<T>>(1);
        let msg = RequestMessage{contents, snd};
        self.sender.clone().send(msg)
            .map(|_| debug!("Successfully sent request to map"))
            .map_err(|_| "Could not sent request to map")
            .and_then(|_| rcv.collect().map_err(|_| "could not collect from receiver"))
            .map(|mut replies| {
                let reply = replies.remove(0);
                reply
            }).map_err(|e| e.to_string())
    }

    pub fn get(&self, key: K) -> impl Future< Item = MapperReply<T>, Error = String> {
        let msg = Contents::Fetch(FetchContents {key});
        self.send_request(msg)
    }

    pub fn set(&self, key: K, data: T) -> impl Future< Item = MapperReply<T>, Error = String> {
        let msg = Contents::Place(PlaceContents {key, data});
            self.send_request(msg)
    }

    pub fn receive(&mut self) -> Result<impl Future<Item = (), Error = ()>, String> {
        let mut map = self.map.take().ok_or("Receive Future already created")?;
        let receiver = self.receiver.take().ok_or("Receive Future already created")?;
        Ok(receiver.for_each(move |msg| {
            match msg.contents {
                Contents::Fetch(fetch) => {
                    info!("Received a Fetch request");
                    match map.get(&fetch.key) {
                        Some(found_data) => {
                            let data = found_data.clone();
                            msg.snd.send(MapperReply::Data(DataContents { data }))
                        },
                        None => msg.snd.send(MapperReply::NotFound)
                    }
                },
                Contents::Place(place) => {
                    info!("Received a Place request");
                    map.insert(place.key, Arc::new(place.data));
                    msg.snd.send(MapperReply::Ok)
                }
            }
            .map(|_| info!("replied to request"))
            .map_err(|_| info!("failed to reply to request"))
        }))
    }
}
