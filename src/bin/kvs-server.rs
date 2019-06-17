#[macro_use]
extern crate clap;
use clap::ArgMatches;

extern crate slog;
extern crate slog_term;
extern crate slog_async;
use slog::*;

use std::net::{ TcpListener, TcpStream };
use std::io::*;

extern crate kvs;
use kvs::{ Result, KvStore };

use failure::err_msg;

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

fn handle_connection(mut log: Logger, mut stream: TcpStream, store: &mut KvStore) -> Result<()> {

    let mut br = BufReader::new(stream.try_clone()?);

    let mut request = String::new();
    br.read_line(&mut request)?;

    log = log.new(o!("net_request" => request.clone()));
    info!(log, "Operation recieved from client");

    let operation = parse_request(log.clone(), request)?;

    let op_result = handle_operation(log.clone(), operation, store);

    let response = match op_result {
        Ok(Some(value)) => {
            format!("OK {}", value)
        },
        Ok(None) => {
            String::from("OK")
        },
        Err(_) => {
            String::from("FAIL")
        }
    };
    log = log.new(o!("response" => response.clone()));
    writeln!(stream, "{}", response)?;
    info!(log, "Response sent to client");

    Ok(())
}

#[derive(Debug, Clone)]
enum Operation {
    Set(String, String),
    Get(String),
    Remove(String)
}

impl KV for Operation {
    fn serialize(&self, _record: &Record, serializer: &mut Serializer) -> slog::Result<()> {
        match self {
            Operation::Set(key, value) => {

                serializer.emit_str("parsed_operation", &format!("Set {}->{}", key, value))?;
                
            }
            Operation::Get(key) => {

                serializer.emit_str("parsed_operation", &format!("Get {}", key))?;
                
            }
            Operation::Remove(key) => {

                serializer.emit_str("parsed_operation", &format!("Remove {}", key))?;
                
            }
        }
        Ok(())
    }
}

fn parse_request(mut log: Logger, request: String) -> Result<Operation> {
    let request = remove_newline_from_end(request);
    let v: Vec<&str> = request.split(' ').collect();

    if v[0] == "set" {
        
        let key = v[1];
        let value = v[2];
        let op = Operation::Set(String::from(key), String::from(value));
        log = log.new(o!(op.clone()));
        info!(log, "Request parsed");
        Ok(op)
        

    } else if v[0] == "get" {

        let key = v[1];
        let op = Operation::Get(String::from(key));
        log = log.new(o!(op.clone()));
        info!(log, "Request parsed");
        Ok(op)

    } else if v[0] == "rm" {

        let key = v[1];
        let op = Operation::Remove(String::from(key));
        log = log.new(o!(op.clone()));
        info!(log, "Request parsed");
        Ok(op)

    } else {
        Err(err_msg("Request does not start with a valid operation code"))
    }
}

fn remove_newline_from_end(string: String) -> String {
    let len = string.len();

    let halves = string.split_at(len - 1);

    if halves.1 == "\n" {
        String::from(halves.0)
    } else {
        string
    }
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