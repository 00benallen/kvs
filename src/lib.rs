//! KvStore library for use by the kvs CLI
//#![deny(missing_docs)]

mod engine;
use std::sync::{
    Arc,
    Mutex
};
pub use engine::KvsEngine;
pub use engine::SledKvsEngine;

/// Module contains structs which define the network protocol between KvsClient and KvsServer
pub mod network;

pub mod thread_pool;

use std::path;
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use std::io::prelude::*;
use std::io::{ BufWriter, BufReader };
use std::fs::{ File, OpenOptions, create_dir };
use failure::err_msg;
use std::collections::HashMap;

/// Result type returned by KvStore
pub type Result<T> = std::result::Result<T, failure::Error>;

/// Represents a Key/Value Pair, elementary data stored by the KvStore
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Pair {
    k: String,
    v: String,
}

/// Commands which KvStore enters into log
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Command {
    /// Set the value of a Pair, or add a new one
    Set(Pair),

    /// Remove a Pair
    Remove(String)
}

/// Store for storing key value pair
#[derive(Clone)]
pub struct KvStore {
    index: Arc<Mutex<HashMap<String, usize>>>,
    log_path: PathBuf,
    log_threshold: i32
}


impl KvStore {

    /// Creates a new empty KvStore with a default log file in the current directory
    pub fn new() -> Result<KvStore> {

        let log_folder = "./";
        let log_path = PathBuf::from(log_folder);

        if !log_path.exists() {
            create_dir(log_folder)?;
        }

        KvStore::open(&log_path)
    }

    /// Create a new empty KvStore with a log file in the specified directory
    pub fn open(path: &path::Path) -> Result<KvStore> {

        let mut log_path = PathBuf::from(path);
        log_path.push("log.log");

        let mut store = KvStore { 
            index: Arc::new(Mutex::new(HashMap::new())),
            log_path,
            log_threshold: 500,
        };
        store.generate_index()?;

        Ok(store)
    }

    /// Create an index of key -> file offsets for storage in memory. This makes reads much faster
    /// Must be regenerated on each write
    fn generate_index(&mut self) -> Result<()> {
        let br = self.open_reader()?;

        //TODO add back log compaction on its own thread
        let index = &mut self.index.lock().unwrap();
        // let mut should_compact_log = false;
        for (offset, line) in br.lines().enumerate() {
            let line = line?;
            let command = serde_json::from_str(&line)?;
            match command {
                Command::Set(pair) => {
                    index.insert(pair.k, offset);
                },
                Command::Remove(key) => {
                    index.remove(&key);
                }
            }

            // if offset > self.log_threshold as usize {

            //     should_compact_log = true;

            // }
        }

        // if should_compact_log {
        //     self.compact_log()?;
        // }

        Ok(())
    }

    // fn compact_log(&mut self) -> Result<()> {

    //     let br = self.open_reader()?;

    //     let mut new_log: Vec<Command> = Vec::new();
    //     for line in br.lines() {

    //         let line = line?;
    //         let command: Command = serde_json::from_str(&line)?;

    //         KvStore::add_or_replace_command_in_vec(&mut new_log, command);
    //     }

    //     let mut bw = self.open_writer(false)?;

    //     for command in new_log.iter() {
    //         let command_json = serde_json::to_string(&command)?;
    //         bw.write_all(command_json.as_bytes())?;
    //         bw.write_all(b"\n")?;
    //     }
    //     bw.flush()?;

    //     Ok(())
    // }

    // fn add_or_replace_command_in_vec(vec: &mut Vec<Command>, command: Command) { 
    //     match command {
    //         Command::Set(pair) => {
    //             let command_dup = Command::Set(Pair { k: pair.k.clone(), v: pair.v.clone() });
    //             let index_opt = vec.iter().position(|c| {
    //                 match c {
    //                     Command::Set(pair_inner) => {
    //                         pair.k == pair_inner.k
    //                     },
    //                     Command::Remove(_) => { false }
    //                 }
    //             });
    //             if let Some(index) = index_opt {
    //                 vec.remove(index);
    //                 vec.push(command_dup);
    //             } else {
    //                 vec.push(command_dup);
    //             }
    //         },
    //         Command::Remove(_) => {
    //             vec.push(command);
    //         }
    //     }
    // }

    fn open_writer(&self, append: bool) -> Result<BufWriter<File>> {
        let f = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .append(append)
        .truncate(!append)
        .open(&self.log_path)?;

        Ok(BufWriter::new(f))
    }

    fn open_reader(&self) -> Result<BufReader<File>> {
        let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&self.log_path)?;

        Ok(BufReader::new(f))
    }
}

impl KvsEngine for KvStore {

    fn set(&self, k: String, v: String) -> Result<()> {
        let command = Command::Set(Pair { k, v });

        let mut bw = self.open_writer(true)?;
        let command_json = serde_json::to_string(&command)?;
        bw.write_all(command_json.as_bytes())?;
        bw.write_all(b"\n")?;
        bw.flush()?;
        
        // TODO see if this is necessary? Trying to get a mutable reference
        // to the index, probably a better way
        let mut clone = self.clone();
        clone.generate_index()?;

        Ok(())

    }

    fn get(&self, k: String) -> Result<Option<String>> {
        
        let index = self.index.lock().unwrap();
        if let Some(offset) = index.get(&k) {

            let br = self.open_reader()?;

            let command_json = br.lines().nth(*offset).ok_or_else(|| err_msg("File pointer in index points to non-existant command"))??;

            let command: Command = serde_json::from_str(&command_json)?;

            match command {
                Command::Set(pair) => {
                    return Ok(Some(pair.v));
                },
                Command::Remove(_) => {
                    return Err(err_msg("File pointer in index points to remove command"));
                }
            }

        } else {
            Ok(None)
        }
    }

    fn remove(&self, k: String) -> Result<()> {
        
        let entry_opt = self.get(k.clone())?;

        if entry_opt.is_some() {

            let mut bw = self.open_writer(true)?;
            let command = Command::Remove(k);
            let command_json = serde_json::to_string(&command)?;
            bw.write_all(command_json.as_bytes())?;
            bw.write_all(b"\n")?;
            bw.flush()?;

            // TODO see if this is necessary? Trying to get a mutable reference
            // to the index, probably a better way
            let mut clone = self.clone();
            clone.generate_index()?;

            Ok(())

        } else {
            Err(err_msg("Key not found"))
        }
    }

}

// extern crate failure;
// #[macro_use] extern crate failure_derive;

// #[derive(Fail, Debug)]
// enum MyError {
//     #[fail(display = "{} is not a valid version.", _0)]
//     InvalidVersion(u32),
//     #[fail(display = "IO error: {}", error)]
//     IoError { error: io::Error },
//     #[fail(display = "An unknown error has occurred.")]
//     UnknownError,
// }
