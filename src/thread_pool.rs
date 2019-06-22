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

pub trait ThreadPool {
    fn new(threads: usize) -> Result<Self> where Self: Sized;
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

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
            self.threads_spawned.fetch_sub(1, Ordering::AcqRel);
        } else {
            println!("Watcher dropped without thread panicking");
        }
    }
}

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
            loop {
                let new_to_spawn = threads - shared_threads_spawned.load(Ordering::Acquire);

                for _ in 0..new_to_spawn {
                    println!("Spawning job thread due to restart");
                    let shared_threads_spawned = threads_spawned.clone();
                    let shared_queue = shared_queue.clone();
                    shared_threads_spawned.fetch_add(1, Ordering::AcqRel);
                    std::thread::spawn(move || {
                        job_thread_closure(shared_queue, shared_threads_spawned)
                    });
                    
                }
            }
        });


        Ok(SharedQueueThreadPool {
            job_queue
        })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.job_queue.lock().expect("Could not send job to threads, job_queue could not be locked").push_front(ThreadPoolMessage::RunJob(Box::new(job)));
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

pub struct RayonThreadPool {
    
}

impl RayonThreadPool {
    
}

impl ThreadPool for RayonThreadPool {
    fn new(thread: usize) -> Result<Self> {
        unimplemented!() // TODO
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        unimplemented!() // TODO
    }
}

