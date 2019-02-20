# Kitap

Kitap is a content addressable store written in rust, using [tokio](https://tokio.rs/).


## How to use

Clone this repo and then build the crate using Cargo:

    cargo build --release

Then start `kitapd`. Use 

    target/release/kitapd -h

to see the required command line options.

Then the client programme  `kitap` can be used to `place` files in the store or `fetch` them. Use

    target/release/kitap -h
    
to see the required command line options.
