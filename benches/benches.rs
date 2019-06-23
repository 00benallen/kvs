#[macro_use]
extern crate criterion;

use criterion::Criterion;

extern crate rand;
use rand::prelude::*;

extern crate kvs;
use kvs::{
    KvStore,
    KvsEngine,
    SledKvsEngine
};

use tempfile::TempDir;

fn kvs_benchmarks(c: &mut Criterion) {

    let mut keys_bytes = [0u8; 100];
    rand::thread_rng().fill_bytes(&mut keys_bytes);

    let keys: Vec<String> = keys_bytes.iter().map(|byte| byte.to_string()).collect();

    let mut values_bytes = [0u8; 100];
    rand::thread_rng().fill_bytes(&mut values_bytes);
    let values: Vec<String> = keys_bytes.iter().map(|byte| byte.to_string()).collect();

    let pairs: Vec<(String, String)> = keys.clone().into_iter().zip(values.into_iter()).collect();
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store_1 = KvStore::open(temp_dir.path()).unwrap();

    c.bench_function_over_inputs("kvs_write", move |b, pairs| {
        b.iter(|| {
            for pair in pairs {
                store_1.set(pair.0.clone(), pair.1.clone()).unwrap();
            }
        });
    },
    vec![pairs]);

    let mut store_2 = KvStore::open(temp_dir.path()).unwrap();
    c.bench_function_over_inputs("kvs_read", move |b, keys| {
        b.iter(|| {
            for key in keys {
                store_2.get(key.clone()).unwrap().unwrap();
            }
        });
    },
    vec![keys]);
    println!("Benchmarks finished");
}

fn sled_benchmarks(c: &mut Criterion) {

    let mut keys_bytes = [0u8; 100];
    rand::thread_rng().fill_bytes(&mut keys_bytes);

    let keys: Vec<String> = keys_bytes.iter().map(|byte| byte.to_string()).collect();

    let mut values_bytes = [0u8; 100];
    rand::thread_rng().fill_bytes(&mut values_bytes);
    let values: Vec<String> = keys_bytes.iter().map(|byte| byte.to_string()).collect();

    let pairs: Vec<(String, String)> = keys.clone().into_iter().zip(values.into_iter()).collect();
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store_1 = SledKvsEngine::open(temp_dir.path()).unwrap();

    c.bench_function_over_inputs("sled_write", move |b, pairs| {
        b.iter(|| {
            for pair in pairs {
                store_1.set(pair.0.clone(), pair.1.clone()).unwrap();
            }
        });
    },
    vec![pairs]);

    let mut store_2 = SledKvsEngine::open(temp_dir.path()).unwrap();
    c.bench_function_over_inputs("sled_read", move |b, keys| {
        b.iter(|| {
            for key in keys {
                store_2.get(key.clone()).unwrap().unwrap();
            }
        });
    },
    vec![keys]);
    println!("Benchmarks finished");
}



criterion_group!(benches, kvs_benchmarks, sled_benchmarks);
criterion_main!(benches);