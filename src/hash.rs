use sha3::{Digest, Sha3_512};
use generic_array::GenericArray;

pub type KitapHash = GenericArray<u8, <Sha3_512 as Digest>::OutputSize>;

pub const HASH_SIZE: usize = 64;

pub struct KitapHasher {
    hasher: Sha3_512
}

impl KitapHasher {
    pub fn new() -> KitapHasher {
        let hasher = Sha3_512::new();
        KitapHasher {
            hasher,
        }
    }

    pub fn input<D: AsRef<[u8]>>(&mut self, data: D) {
        self.hasher.input(data);
    }

    pub fn result(self) -> KitapHash {
        self.hasher.result().into()
    }
}
