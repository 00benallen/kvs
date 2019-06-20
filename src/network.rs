extern crate slog;
extern crate slog_term;
extern crate slog_async;
use slog::*;

use failure::err_msg;

use std::net::TcpStream;
use std::io::*;

use crate::Result;

const SET_CODE: &str = "set";
const GET_CODE: &str = "get";
const REMOVE_CODE: &str = "rm";

/// Trait defining a message to be sent between KvsServer and KvsClient, ensures the object is easy to use
pub trait TcpMessage {

    /// Create an instance from a String
    fn from_text(log: Logger, req: String) -> Result<Self> where Self: Sized;

    /// Convert this instance to a string
    fn to_text(&self) -> String;

    /// Write this instance to the given `TcpStream`
    fn write_to_stream(&self, log: Logger, stream: TcpStream) -> Result<()>;

    /// Read an instance out of a `TcpStream`
    fn read_from_stream(log: Logger, stream: TcpStream) -> Result<Self> where Self: Sized;
}

/// Operations the KvsClient sends to the KvsServer
#[derive(Debug, Clone)]
pub enum Operation {

    /// Set a new Key/Value pair
    Set(String, String),

    /// Retrieve the value for a given key
    Get(String),

    /// Remove a Key/Value pair
    Remove(String)
}

impl TcpMessage for Operation {
    fn from_text(mut log: Logger, req: String) -> Result<Operation> {
        let request = remove_newline_from_end(req);
        let v: Vec<&str> = request.split(' ').collect();

        if v[0] == SET_CODE {
            
            let key = v[1];
            let value = v[2];
            let op = Operation::Set(String::from(key), String::from(value));
            log = log.new(o!(op.clone()));
            info!(log, "Request parsed");
            Ok(op)
            

        } else if v[0] == GET_CODE {

            let key = v[1];
            let op = Operation::Get(String::from(key));
            log = log.new(o!(op.clone()));
            info!(log, "Request parsed");
            Ok(op)

        } else if v[0] == REMOVE_CODE {

            let key = v[1];
            let op = Operation::Remove(String::from(key));
            log = log.new(o!(op.clone()));
            info!(log, "Request parsed");
            Ok(op)

        } else {
            Err(err_msg("Request does not start with a valid operation code"))
        }
    }

    fn to_text(&self) -> String {

        match self {
            Operation::Get(key) => {
                format!("{} {}", GET_CODE, key)
            },
            Operation::Remove(key) => {
                format!("{} {}", REMOVE_CODE, key)
            },
            Operation::Set(key, value) => {
                format!("{} {} {}", SET_CODE, key, value)
            }
        }
    }

    fn write_to_stream(&self, mut log: Logger, mut stream: TcpStream) -> Result<()> {
        let net_operation = self.to_text();
        log = log.new(o!("net_operation" => net_operation.clone()));
        info!(log, "Sending operation to server");
        writeln!(stream, "{}", net_operation)?;
        info!(log, "Operation sent");
        Ok(())
    }

    fn read_from_stream(mut log: Logger, stream: TcpStream) -> Result<Operation> {
        let mut br = BufReader::new(stream.try_clone()?);

        let mut request = String::new();
        br.read_line(&mut request)?;

        log = log.new(o!("net_request" => request.clone()));
        info!(log, "Operation recieved from client");

        Operation::from_text(log.clone(), request)
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

/// Status for a Response sent back by the KvsServer
#[derive(PartialEq)]
pub enum ResponseStatus {

    /// Operation was successful, requested data should be in `Response`
    Ok,

    /// Operation failed
    Fail
}

impl ResponseStatus {

    /// Create a response status from a String
    pub fn from_text(text: String) -> Result<ResponseStatus> {
        let trimmed = remove_newline_from_end(text);
        if trimmed == "OK" {
            Ok(ResponseStatus::Ok)
        } else if trimmed == "FAIL" {
            Ok(ResponseStatus::Fail)
        } else {
            Err(err_msg("Text could not be converted to response status"))
        }
    }
}

/// Response the KvsServer send back to the client
pub struct Response {
    /// Status of the response, see `ResponseStatus` for details
    pub status: ResponseStatus,
    /// Data requested by client, will be None depending on the operation sent
    pub data: Option<String>
}

impl TcpMessage for Response {

    fn from_text(log: Logger, req: String) -> Result<Response> {
        
        info!(log, "Parsing Response from text");
        let v: Vec<&str> = req.split(' ').collect();
        if v.len() == 2 {
            Ok(Response {
                status: ResponseStatus::from_text(String::from(v[0]))?,
                data: Some(String::from(v[1]))
            })
        } else if v.len() == 1 {
            Ok(Response {
                status: ResponseStatus::from_text(String::from(v[0]))?,
                data: None
            })
        } else {
            Err(err_msg("Text could not be parsed to Response"))
        }
        

    }

    fn to_text(&self) -> String {
        match self.status {
            ResponseStatus::Ok => {
                match &self.data {
                    Some(data) => {
                        format!("OK {}", data)
                    },
                    None => {
                        String::from("OK")
                    }
                }
            },
            ResponseStatus::Fail => {
                String::from("FAIL")
            }
        }
    }

    fn write_to_stream(&self, mut log: Logger, mut stream: TcpStream) -> Result<()> {
        let text = self.to_text();
        log = log.new(o!("response" => text.clone()));
        writeln!(stream, "{}", text)?;
        info!(log, "Response written to stream");
        Ok(())
    }

    fn read_from_stream(mut log: Logger, stream: TcpStream) -> Result<Response> {
        let mut br = BufReader::new(stream);
        let mut response_text = String::new();
        br.read_line(&mut response_text)?;

        let response = Response::from_text(log.clone(), response_text.clone())?;

        log = log.new(o!("response" => response_text));
        info!(log, "Response received from server");
        Ok(response)
    }

}

