use crate::Result;

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