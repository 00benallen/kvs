//! KvStore library for use by the kvs CLI
//#![deny(missing_docs)]

mod engine;
pub use engine::KvsEngine;

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
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Pair {
    k: String,
    v: String,
}

/// Commands which KvStore enters into log
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum Command {
    /// Set the value of a Pair, or add a new one
    Set(Pair),

    /// Remove a Pair
    Remove(String)
}

/// Store for storing key value pairs
pub struct KvStore {
    index: HashMap<String, usize>,
    cache: Vec<Command>,
    log_path: PathBuf,
    cached_commands: i32,
    cache_threshold: i32,
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
            index: HashMap::new(),
            cache: Vec::new(),
            log_path,
            cached_commands: 0,
            cache_threshold: 1,
            log_threshold: 500,
        };
        store.generate_index()?;

        Ok(store)
    }

    /// Create an index of key -> file offsets for storage in memory. This makes reads much faster
    /// Must be regenerated on each write
    fn generate_index(&mut self) -> Result<()> {
        let br = self.open_reader()?;

        let index = &mut self.index;
        let mut should_compact_log = false;
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

            if offset > self.log_threshold as usize {

                should_compact_log = true;

            }
        }

        if should_compact_log {
            self.compact_log()?;
        }

        Ok(())
    }

    fn compact_log(&mut self) -> Result<()> {

        let br = self.open_reader()?;

        let mut new_log: Vec<Command> = Vec::new();
        for line in br.lines() {

            let line = line?;
            let command: Command = serde_json::from_str(&line)?;

            KvStore::add_or_replace_command_in_vec(&mut new_log, command);
        }

        let mut bw = self.open_writer(false)?;

        for command in new_log.iter() {
            let command_json = serde_json::to_string(&command)?;
            bw.write_all(command_json.as_bytes())?;
            bw.write_all(b"\n")?;
        }
        bw.flush()?;

        Ok(())
    }

    fn add_or_replace_command_in_vec(vec: &mut Vec<Command>, command: Command) { 
        match command {
            Command::Set(pair) => {
                let command_dup = Command::Set(Pair { k: pair.k.clone(), v: pair.v.clone() });
                let index_opt = vec.iter().position(|c| {
                    match c {
                        Command::Set(pair_inner) => {
                            pair.k == pair_inner.k
                        },
                        Command::Remove(_) => { false }
                    }
                });
                if let Some(index) = index_opt {
                    vec.remove(index);
                    vec.push(command_dup);
                } else {
                    vec.push(command_dup);
                }
            },
            Command::Remove(_) => {
                vec.push(command);
            }
        }
    }

    /// Sets a value to a key in the store, will add a new K/V entry if none exists,
    /// otherwise will overwrite an existing entry
    pub fn set(&mut self, k: String, v: String) -> Result<()> {
        let command = Command::Set(Pair { k, v });

        self.cache.push(command);

        self.raise_cached_commands(1)?;

        Ok(())

    }

    fn raise_cached_commands(&mut self, amount: i32) -> Result<()> {

        self.cached_commands += amount;

        if self.cached_commands >= self.cache_threshold {
            self.write_cached_to_log()?;
        }
        Ok(())
    }

    fn write_cached_to_log(&mut self) -> Result<()> {

        if self.cached_commands <= 0 {
            return Ok(());
        }

        let cached_start = self.cache.len() as i32 - self.cached_commands;
        
        let latest_index = (self.cache.len() - 1) as i32;
        let mut bw = self.open_writer(true)?;
        for i in cached_start..=latest_index {
            let command = &self.cache[i as usize];
            let command_json = serde_json::to_string(&command)?;
            bw.write_all(command_json.as_bytes())?;
            bw.write_all(b"\n")?;
        }
        bw.flush()?;
        self.cached_commands = 0;
        self.cache.clear();
        self.generate_index()?;
        
        Ok(())

    }

    /// Get the value for a key in the store. Will return Some(value) if it exists,
    /// otherwise will return None
    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        
        if let Some(offset) = self.index.get(&k) {

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

    /// Remove a K/V entry from the store, will do nothing if the entry doesn't exist
    pub fn remove(&mut self, k: String) -> Result<()> {
        
        let entry_opt = self.get(k.clone())?;

        if entry_opt.is_some() {

            let command = Command::Remove(k);
            self.cache.push(command);

            self.raise_cached_commands(1)?;

            Ok(())

        } else {
            Err(err_msg("Key not found"))
        }

        
    }

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

impl Drop for KvStore {

    fn drop(&mut self) {
        self.write_cached_to_log().expect("Could not save cache to disk");
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
