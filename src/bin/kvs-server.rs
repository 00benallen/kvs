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

extern crate num_cpus;

extern crate kvs;
use kvs::{ 
    Result, 
    KvStore,
    KvsEngine,
    SledKvsEngine,
    network::{
        Operation,
        TcpMessage,
        Response,
        ResponseStatus
    },
    thread_pool::{
        ThreadPool,
        SharedQueueThreadPool,
        NaiveThreadPool,
        RayonThreadPool
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
        (@arg ADDRESS: --addr +takes_value "Address to listen to")
        (@arg ENGINE: --engine +takes_value "Backend engine to use")
        (@arg THREADPOOL: --tp +takes_value "Thread pool implementation to use")
    )
    .get_matches();

    let address = matches.value_of("ADDRESS").unwrap_or("127.0.0.1:4000");
    let engine = matches.value_of("ENGINE").unwrap_or("kvs");
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
    } else if buf.is_empty() {
        engine_file.write_all(engine.as_bytes())?;
    }

    let thread_pool_type = matches.value_of("THREADPOOL").unwrap_or("queued");

    match thread_pool_type {
        "naive" => {
            start_server(log.clone(),  NaiveThreadPool::new(0)?, address, engine)?;
        },
        "queued" => {
            start_server(log.clone(),  SharedQueueThreadPool::new(num_cpus::get())?, address, engine)?;
        },
        "rayon" => {
            start_server(log.clone(),  RayonThreadPool::new(num_cpus::get())?, address, engine)?;
        },
        _ => { return Err(err_msg("Invalid thread pool type")) }
    }

    info!(log, "Server terminating");
    Ok(())
}

fn start_server<Pool: ThreadPool>(log: Logger, tp: Pool, address: &str, engine: &str) -> Result<()> {
    match engine {
        "kvs" => {
            listen_for_connections(log, address, KvStore::new()?, tp)?;
        },
        "sled" => {
            listen_for_connections(log, address, SledKvsEngine::new()?, tp)?;
        },
        _ => { return Err(err_msg("Invalid engine type")) }
    }
    Ok(())
}

fn listen_for_connections<Engine: KvsEngine, Pool: ThreadPool>(mut log: Logger, address: &str, store: Engine, tp: Pool) -> Result<()> {
    info!(log, "Starting TCP server");
    let listener = TcpListener::bind(address)?;
    info!(log, "Waiting for connections...");

    for stream in listener.incoming() {
        let stream: TcpStream = stream?;
        let client_addr = stream.peer_addr()?;

        log = log.new(o!("client_addr" => client_addr));
        info!(log, "TCP connection established");
        let store = store.clone();
        let log = log.clone();

        tp.spawn(move || handle_connection(log, stream, store));
        
    }
    Ok(())
}

fn handle_connection<Engine: KvsEngine>(log: Logger, stream: TcpStream, store: Engine) {

    let operation = Operation::read_from_stream(log.clone(), stream.try_clone().unwrap()).unwrap();

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

    response.write_to_stream(log, stream).unwrap();
}

fn handle_operation<Engine: KvsEngine>(log: Logger, operation: Operation, store: Engine) -> Result<Option<String>> {

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