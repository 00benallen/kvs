#[macro_use]
extern crate clap;
use clap::ArgMatches;

extern crate slog;
extern crate slog_term;
extern crate slog_async;
use slog::*;

use std::net::{ TcpListener, TcpStream };

use std::io::prelude::*;
use std::fs::{ OpenOptions };

use failure::err_msg;

extern crate kvs;
use kvs::{ 
    Result, 
    KvStore,
    KvsEngine,
    network::{
        Operation,
        TcpMessage,
        Response,
        ResponseStatus
    }
};

fn initialize_root_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().stderr().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!("app_name" => "kvs-server", "version" => env!("CARGO_PKG_VERSION")))
}

fn main() -> Result<()> {

    let mut log = initialize_root_logger();
    info!(log, "Starting up!");
    
    let version = env!("CARGO_PKG_VERSION");
    let author = env!("CARGO_PKG_AUTHORS");
    let about = env!("CARGO_PKG_DESCRIPTION");
    let matches: ArgMatches = clap_app!(kvs =>
        (version: version)
        (author: author)
        (about: about)
        (@arg ADDRESS: --addr +required +takes_value "Address to listen to")
        (@arg ENGINE: --engine +required +takes_value "Backend engine to use")
        
    )
    .get_matches();

    let address = matches.value_of("ADDRESS").expect("Required field address not retrieved");
    let engine = matches.value_of("ENGINE").expect("Required field engine not retrieved");
    log = log.new(o!("address" => String::from(address), "engine" => String::from(engine)));
    info!(log, "Command line arguments read");

    let mut engine_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .append(true)
        .truncate(false)
        .open("./engine")?;
    let buf = &mut String::new();
    engine_file.read_to_string(buf)?;

    if buf != engine && !buf.is_empty() {
        return Err(err_msg("Server cannot be started in a different engine than before"));
    } else {
        engine_file.write_all(engine.as_bytes())?;
    }

    info!(log, "Starting TCP server");
    let listener = TcpListener::bind(address)?;
    info!(log, "Waiting for connections...");

    let mut store = KvStore::new()?;
    info!(log, "Initialized KvStore");

    for stream in listener.incoming() {
        let stream: TcpStream = stream?;
        let client_addr = stream.peer_addr()?;

        log = log.new(o!("client_addr" => client_addr));
        info!(log, "TCP connection established");

        handle_connection(log.clone(), stream, &mut store)?;
    }

    info!(log, "Server terminating");
    Ok(())
}

fn handle_connection(log: Logger, stream: TcpStream, store: &mut KvStore) -> Result<()> {

    let operation = Operation::read_from_stream(log.clone(), stream.try_clone()?)?;

    let op_result = handle_operation(log.clone(), operation, store);

    let response = match op_result {
        Ok(data) => {
            Response {
                status: ResponseStatus::Ok,
                data
            }
        },
        Err(_) => {
            Response {
                status: ResponseStatus::Fail,
                data: None
            }
        }
    };

    response.write_to_stream(log, stream)?;

    Ok(())
}

fn handle_operation(log: Logger, operation: Operation, store: &mut KvStore) -> Result<Option<String>> {

    match operation {
        Operation::Set(key, value) => {
            store.set(key, value)?;
            info!(log, "Store SET successful");
            Ok(None)
        },
        Operation::Get(key) => {
            let result = Ok(store.get(key)?);
            info!(log, "Store GET successful");
            result
        },
        Operation::Remove(key) => {
            store.remove(key)?;
            info!(log, "Store REMOVE successful");
            Ok(None)
        },
    }
    
}