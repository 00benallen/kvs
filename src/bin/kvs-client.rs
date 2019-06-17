#[macro_use]
extern crate clap;
use clap::ArgMatches;

extern crate slog;
extern crate slog_term;
extern crate slog_async;
use slog::*;

extern crate kvs;
use kvs::{ Result };

use std::net::{ TcpStream };
use std::time::Duration;
use std::io::*;

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
            (@arg ADDRESS: --addr +required +takes_value "Address to send to")
        )
        (@subcommand get =>
            (about: "Get the string value of a given string key")
            (@arg KEY: +required "The string key used to store the value")
            (@arg ADDRESS: --addr +required +takes_value "Address to send to")
        )
        (@subcommand rm =>
            (about: "Remove a given key")
            (@arg KEY: +required "The string key to store with")
            (@arg ADDRESS: --addr +required +takes_value "Address to send to")
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

        let net_command = format!("set {} {}", key, value);
        write_command_to_stream(log.clone(), stream.try_clone()?, net_command)?;

        let response = read_response_from_stream(log.clone(), stream.try_clone()?)?;

        if response == "OK\n" {
            Ok(())
        } else {
            Err(err_msg("Error response recieved from server"))
        }
        

    } else if let Some(matches) = matches.subcommand_matches("get") {

        let key = matches.value_of("KEY").expect("Required field KEY not retrieved");

        log = log.new(o!("subcommand" => "get", "key" => String::from(key)));
        info!(log, "CLI arguments processed");

        let stream = open_stream(log.clone(), matches)?;

        let net_command = format!("get {}", key);
        write_command_to_stream(log.clone(), stream.try_clone()?, net_command)?;

        let response = read_response_from_stream(log.clone(), stream.try_clone()?)?;
        let v: Vec<&str> = response.split(' ').collect();

        if v[0] == "OK" {
            let value = v[1];
            print!("{}", value);
            Ok(())
        } else {
            println!("Key not found");
            Ok(())
        }

        

    } else if let Some(matches) = matches.subcommand_matches("rm") {

        let key = matches.value_of("KEY").expect("Required field KEY not retrieved");

        log = log.new(o!("subcommand" => "rm", "key" => String::from(key)));
        info!(log, "CLI arguments processed");

        let stream = open_stream(log.clone(), matches)?;

        let net_command = format!("rm {}", key);
        write_command_to_stream(log.clone(), stream.try_clone()?, net_command)?;

        let response = read_response_from_stream(log.clone(), stream.try_clone()?)?;
        if response == "OK\n" {
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
    let address = matches.value_of("ADDRESS").expect("Required field address not retrieved");
    log = log.new(o!("address" => String::from(address)));
    info!(log, "Server address read");

    info!(log, "Opening TCP connection...");
    let stream = TcpStream::connect_timeout(&address.parse()?, Duration::from_secs(5))?;
    
    log = log.new(o!("server_addr" => stream.peer_addr()?));
    info!(log, "TCP connection established");

    Ok(stream)
}

fn write_command_to_stream(mut log: Logger, mut stream: TcpStream, net_command: String) -> Result<()> {
    log = log.new(o!("net_command" => net_command.clone()));
    info!(log, "Sending command to server");
    writeln!(stream, "{}", net_command)?;
    info!(log, "Command sent, waiting for response");
    Ok(())
}

fn read_response_from_stream(mut log: Logger, stream: TcpStream) -> Result<String> {

    let mut br = BufReader::new(stream);
    let mut response = String::new();
    br.read_line(&mut response)?;

    log = log.new(o!("response" => response.clone()));
    info!(log, "Response recieved from server");
    Ok(response)
}
