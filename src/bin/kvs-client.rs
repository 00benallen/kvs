#[macro_use]
extern crate clap;
use clap::ArgMatches;

extern crate slog;
extern crate slog_term;
extern crate slog_async;
use slog::*;

extern crate kvs;
use kvs::{ 
    Result,
    network::{ 
        Operation,
        TcpMessage,
        Response,
        ResponseStatus
    }
};

use std::net::{ TcpStream };
use std::time::Duration;

use failure::err_msg;

fn initialize_root_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().stderr().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!("app_name" => "kvs-client", "version" => env!("CARGO_PKG_VERSION")))
}

fn main() -> Result<()>{
    let mut log = initialize_root_logger();
    info!(log, "Starting up!");

    let version = env!("CARGO_PKG_VERSION");
    let author = env!("CARGO_PKG_AUTHORS");
    let about = env!("CARGO_PKG_DESCRIPTION");
    let matches: ArgMatches = clap_app!(kvs =>
        (version: version)
        (author: author)
        (about: about)
        (@subcommand set =>
            (about: "Set the value of a string key to a string")
            (@arg KEY: +required "The string key to store with")
            (@arg VALUE: +required "The value to store")
            (@arg ADDRESS: --addr +takes_value "Address to send to")
        )
        (@subcommand get =>
            (about: "Get the string value of a given string key")
            (@arg KEY: +required "The string key used to store the value")
            (@arg ADDRESS: --addr +takes_value "Address to send to")
        )
        (@subcommand rm =>
            (about: "Remove a given key")
            (@arg KEY: +required "The string key to store with")
            (@arg ADDRESS: --addr +takes_value "Address to send to")
        )
    )
    .get_matches();

    // You can handle information about subcommands by requesting their matches by name
    // (as below), requesting just the name used, or both at the same time
    if let Some(matches) = matches.subcommand_matches("set") {

        let key = matches.value_of("KEY").expect("Required field KEY not retrieved");
        let value = matches.value_of("VALUE").expect("Required field VALUE not retrieved");

        log = log.new(o!("subcommand" => "set", "key" => String::from(key), "value" => String::from(value)));
        info!(log, "CLI arguments processed");

        let stream = open_stream(log.clone(), matches)?;

        let operation = Operation::Set(String::from(key), String::from(value));
        operation.write_to_stream(log.clone(), stream.try_clone()?)?;

        let response = Response::read_from_stream(log, stream)?;

        if response.status == ResponseStatus::Ok {
            Ok(())
        } else {
            Err(err_msg("Error response recieved from server"))
        }
        

    } else if let Some(matches) = matches.subcommand_matches("get") {

        let key = matches.value_of("KEY").expect("Required field KEY not retrieved");

        log = log.new(o!("subcommand" => "get", "key" => String::from(key)));
        info!(log, "CLI arguments processed");

        let stream = open_stream(log.clone(), matches)?;

        let operation = Operation::Get(String::from(key));
        operation.write_to_stream(log.clone(), stream.try_clone()?)?;

        let response = Response::read_from_stream(log, stream)?;

        if response.status == ResponseStatus::Ok {

            match response.data {
                Some(value) => {
                    print!("{}", value);
                    Ok(())
                },
                None => {
                    println!("Key not found");
                    Ok(())
                }
            }
        } else {
            std::process::exit(1);
        }

        

    } else if let Some(matches) = matches.subcommand_matches("rm") {

        let key = matches.value_of("KEY").expect("Required field KEY not retrieved");

        log = log.new(o!("subcommand" => "rm", "key" => String::from(key)));
        info!(log, "CLI arguments processed");

        let stream = open_stream(log.clone(), matches)?;

        let operation = Operation::Remove(String::from(key));
        operation.write_to_stream(log.clone(), stream.try_clone()?)?;

        let response = Response::read_from_stream(log, stream)?;
        if response.status == ResponseStatus::Ok {
            Ok(())
        } else {
            eprintln!("Key not found");
            std::process::exit(1);
        }

    } else {
        info!(log, "Sub command not recognized");
        std::process::exit(1);
    }
}

fn open_stream(mut log: Logger, matches: &ArgMatches) -> Result<TcpStream> {
    let address = matches.value_of("ADDRESS").unwrap_or("127.0.0.1:4000");
    log = log.new(o!("address" => String::from(address)));
    info!(log, "Server address read");

    info!(log, "Opening TCP connection...");
    let stream = TcpStream::connect_timeout(&address.parse()?, Duration::from_secs(5))?;
    
    log = log.new(o!("server_addr" => stream.peer_addr()?));
    info!(log, "TCP connection established");

    Ok(stream)
}
