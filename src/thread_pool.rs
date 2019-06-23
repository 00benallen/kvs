//! Thread pool utility to make multithreading in KVS simpler
use std::collections::VecDeque;
use std::sync::{
    Arc,
    Mutex,
    atomic::{
        AtomicUsize,
        Ordering
    },
};
use crate::Result;

/// Trait for a thread pool
pub trait ThreadPool {

    /// Create a new ThreadPool instance
    fn new(threads: usize) -> Result<Self> where Self: Sized;

    /// Pass a job to the ThreadPool
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

/// Thread pool which doesn't actually pool threads
/// Just spawns new a thread for each job given
pub struct NaiveThreadPool {

}

impl NaiveThreadPool {
    
}

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: usize) -> Result<Self> {
        Ok(NaiveThreadPool {

        })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        std::thread::spawn(move || {
            job();
        });
    }
}

type FnOnceBox = Box<FnOnce() + Send + 'static>;
type JobQueue = Arc<Mutex<VecDeque<ThreadPoolMessage>>>;

enum ThreadPoolMessage {
    RunJob(FnOnceBox),
    Shutdown //TODO
}

struct ThreadWatcher {
    threads_spawned: Arc<AtomicUsize>
}

impl Drop for ThreadWatcher {
    fn drop(&mut self) {
        if std::thread::panicking() {
            println!("Thread panicked, reducing number of threads spawned for watcher thread");
            self.threads_spawned.fetch_sub(1, Ordering::Relaxed);
        } else {
            println!("Watcher dropped without thread panicking");
        }
    }
}

/// Implementation of [`ThreadPool`](trait.ThreadPool.html)
/// 
/// # Example
/// ```
/// use kvs::thread_pool::{
///     ThreadPool,
///     SharedQueueThreadPool
/// };
/// 
/// let tp = SharedQueueThreadPool::new(4).unwrap();
/// tp.spawn(|| println!("Job done!"));
/// ```
pub struct SharedQueueThreadPool {
    job_queue: JobQueue,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: usize) -> Result<Self> {

        let job_queue = Arc::new(Mutex::new(VecDeque::new()));
        let threads_spawned = Arc::new(AtomicUsize::new(threads));

        println!("Starting up job threads");
        for _ in 0..threads {
            println!("Spawning job thread");
            let shared_queue = job_queue.clone();
            let shared_threads_spawned = threads_spawned.clone();
            std::thread::spawn(move || {
                job_thread_closure(shared_queue, shared_threads_spawned);
            });
        }

        println!("Starting up watcher thread");
        let shared_queue = job_queue.clone();
        let shared_threads_spawned = threads_spawned.clone();
        std::thread::spawn(move || {
            watcher_thread_closure(threads, shared_queue, shared_threads_spawned);
        });


        Ok(SharedQueueThreadPool {
            job_queue
        })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.job_queue.lock().expect("Could not send job to threads, job_queue could not be locked").push_front(ThreadPoolMessage::RunJob(Box::new(job)));
    }
}

fn watcher_thread_closure(threads: usize, job_queue: JobQueue, threads_spawned: Arc<AtomicUsize>) {
    loop {
        let new_to_spawn = threads - threads_spawned.load(Ordering::Relaxed);

        for _ in 0..new_to_spawn {
            println!("Spawning job thread due to restart");
            let shared_threads_spawned = threads_spawned.clone();
            let shared_queue = job_queue.clone();
            shared_threads_spawned.fetch_add(1, Ordering::Relaxed);
            std::thread::spawn(move || {
                job_thread_closure(shared_queue, shared_threads_spawned)
            });
            
        }
    }
}

fn job_thread_closure(job_queue: JobQueue, threads_spawned: Arc<AtomicUsize>) {
    let _watcher = ThreadWatcher { threads_spawned };
    loop {
        
        let mut job_queue = job_queue.lock().expect("Job thread could not lock job_queue");
        let message_exists = job_queue.pop_front();
        
        if let Some(message) = message_exists {
            
            match message {
                ThreadPoolMessage::RunJob(job) => {
                    println!("Handling next job, {} in queue", job_queue.len());
                    drop(job_queue);
                    job();
                },
                ThreadPoolMessage::Shutdown => {
                    break;
                }
            }
        }
    }
}

extern crate rayon;
use rayon::prelude::*;

/// Thread pool implementation which uses Rayon under the hood, for benchmarking
pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(thread: usize) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new().num_threads(thread).build()?;

        Ok(RayonThreadPool {
            pool
        })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.pool.install(job);
    }
}

