use crate::Result;

/// Trait for defining the interface of a Key/Value store
pub trait KvsEngine {

    /// Sets a value to a key in the store, will add a new K/V entry if none exists,
    /// otherwise will overwrite an existing entry
    fn set(&mut self, k: String, v: String) -> Result<()>;

    /// Get the value for a key in the store. Will return Some(value) if it exists,
    /// otherwise will return None
    fn get(&mut self, k: String) -> Result<Option<String>>;

    /// Remove a K/V entry from the store, will do nothing if the entry doesn't exist
    fn remove(&mut self, k: String) -> Result<()>;
    
}

use sled::{ Db, IVec };
use std::path;
use std::path::PathBuf;
use std::sync::Arc;
use std::fs::create_dir;
use std::str::from_utf8;
use sled::Error;

/// Implementation of KvsEngine which uses the `sled` crate as its backend
pub struct SledKvsEngine {
    tree: Db,
}

impl SledKvsEngine {

    /// Get a new SledKvsEngine instance, uses the current directory for file storage
    pub fn new() -> Result<SledKvsEngine> {

        let log_folder = "./";
        let log_path = PathBuf::from(log_folder);

        if !log_path.exists() {
            create_dir(log_folder)?;
        }

        Self::open(&log_path)
    }

    /// Get a new SledKvsEngine instance, uses the given path for file storage
    pub fn open(path: &path::Path) -> Result<SledKvsEngine> {
        
        let tree = Db::start_default(path)?;

        Ok(SledKvsEngine {
            tree
        })

    }

    fn convert_sled_result(sled_result: std::result::Result<Option<IVec>, Error>) -> Result<Option<String>> {
        Ok(sled_result.map(|o: Option<IVec>| {
            o.map(|v| {
                let bytes: Arc<[u8]> = v.into();
                String::from(from_utf8(bytes.as_ref()).expect("Value is corrupted"))
            })
        })?)
    }
}

impl KvsEngine for SledKvsEngine {

    fn set(&mut self, k: String, v: String) -> Result<()> {
        self.tree.set(k.as_bytes(), v.as_bytes())?;
        Ok(())
    }

    fn get(&mut self, k: String) -> Result<Option<String>> {
        let result = self.tree.get(k.as_bytes());

        //TODO cleanup, converting from annoying sled type to string was complex
        SledKvsEngine::convert_sled_result(result)
    }

    fn remove(&mut self, k: String) -> Result<()> {
        self.tree.del(k.as_bytes())?;
        Ok(())
    }

}