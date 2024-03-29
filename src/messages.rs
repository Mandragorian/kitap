use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::hash::HASH_SIZE;

static NOTFOUND: [u8; 5] = [78, 84, 70, 78, 68];

pub const MSG_HEADER_LEN: usize = 6;

#[derive(Debug)]
pub enum MessageType {
    Place,
    Fetch,
    NotFound,
    Unknown
}

impl From<u16> for MessageType {
    fn from(t: u16) -> MessageType {
        match t {
            0 => MessageType::Place,
            1 => MessageType::Fetch,
            _ => MessageType::Unknown,
        }
    }
}

impl Into<u16> for MessageType {
    fn into(self) -> u16 {
        match self {
            MessageType::Place => 0,
            MessageType::Fetch => 1,
            MessageType::NotFound => 2,
            MessageType::Unknown => 255,
        }
    }
}

/// A trait for struct that can be sent as a Kitap message.
pub trait Message {
    fn get_type(&self) -> MessageType;
    fn get_contents(&self) -> Vec<u8>;

    fn into_bytes(&self) -> Vec<u8> {
        let msg_type = self.get_type();
        let contents = self.get_contents();
        let len = contents.len();
        let mut v = Vec::with_capacity(2 + len);
        v.write_u16::<LittleEndian>(msg_type.into()).unwrap();
        v.write_u32::<LittleEndian>(len as u32).unwrap();
        v.extend(contents);
        v
    }
}

/// A message for Fetch requests
pub struct FetchMessage {
    hashes: Vec<Vec<u8>>,
}

/// A message for Place requests
pub struct PlaceMessage
{
    pub hash: Vec<u8>,
    pub datasize: usize,
}

impl FetchMessage {
    pub fn new(hashes: Vec<Vec<u8>>) -> FetchMessage {
        FetchMessage {
            hashes,
        }
    }
}

impl Message for FetchMessage {
    fn get_type(&self) -> MessageType {
        MessageType::Fetch
    }

    fn get_contents(&self) -> Vec<u8> {
        self.hashes.join(&(':' as u8))
    }
}

impl PlaceMessage
{
    pub fn new(hash: Vec<u8>, datasize: usize) -> PlaceMessage 
    {
        PlaceMessage {
            hash,
            datasize,
        }
    }

    pub fn try_from(mut buf: Vec<u8>) -> Result<PlaceMessage, String> {
        let datasize = Cursor::new(buf.split_off(HASH_SIZE))
            .read_u32::<LittleEndian>()
            .or(Err("Could not read datasize from buffer"))? as usize;
        let hash = buf;
        Ok(PlaceMessage {
            datasize,
            hash,
        })
    }

}

impl Message for PlaceMessage
{
    fn get_type(&self) -> MessageType {
        MessageType::Place
    }

    fn get_contents(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.hash.len() + 4);
        v.extend(&self.hash);
        v.write_u32::<LittleEndian>(self.datasize as u32).unwrap();
        v
    }
}

pub struct NotFoundMessage<'a> {
    key: &'a Vec<u8>,
}

impl<'a> NotFoundMessage<'a> {
    pub fn new(key: &Vec<u8>) -> NotFoundMessage {
        NotFoundMessage {
            key,
        }
    }
}

impl<'a> Message for NotFoundMessage<'a> {
    fn get_type(&self) -> MessageType {
        MessageType::NotFound
    }

    fn get_contents(&self) -> Vec<u8> {
        self.key.clone()
    }
}
