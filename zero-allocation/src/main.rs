use std::cell::UnsafeCell;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Fixed size for all message buffers
const BUFFER_SIZE: usize = 1024;
/// Number of buffers in the pool (must be <= 64)
const POOL_SIZE: usize = 32;

/// A thread-safe wrapper around a buffer that allows interior mutability
struct Buffer {
    data: UnsafeCell<[u8; BUFFER_SIZE]>,
}

// Manually implement Send and Sync for Buffer
// This is safe because we manage access through our buffer pool's free list
unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Buffer {
    /// Create a new buffer initialized with zeros
    fn new() -> Self {
        Self {
            data: UnsafeCell::new([0; BUFFER_SIZE]),
        }
    }

    unsafe fn get(&self, len: usize) -> &[u8] {
        let ptr = self.data.get();
        unsafe { std::slice::from_raw_parts(ptr as *const u8, len.min(BUFFER_SIZE)) }
    }
}

/// A pre-allocated pool of message buffers that allows zero-allocation message handling
struct MessagePool {
    /// Pre-allocated buffers of fixed size
    buffers: Box<[Buffer; POOL_SIZE]>,
    /// Bitmap to track which buffers are free (1 = free, 0 = in use)
    /// Each bit represents one buffer, allowing atomic operations
    free_list: AtomicU64,
    /// Counters for performance metrics
    total_allocations: AtomicUsize,
    failed_allocations: AtomicUsize,
    current_in_use: AtomicUsize,
    max_in_use: AtomicUsize,
}

/// A reference to a buffer in the pool
struct MessageBuffer<'a> {
    pool: &'a MessagePool,
    index: usize,
    /// Actual length of data in the buffer
    length: usize,
}

impl MessagePool {
    /// Create a new message pool with all buffers marked as available
    fn new() -> Self {
        // Create array of buffers
        let mut buffers = Vec::with_capacity(POOL_SIZE);
        for _ in 0..POOL_SIZE {
            buffers.push(Buffer::new());
        }

        // Convert to fixed-size array
        let buffers = buffers.into_boxed_slice();
        let buffers = unsafe {
            let ptr = Box::into_raw(buffers) as *mut Buffer;
            Box::from_raw(ptr as *mut [Buffer; POOL_SIZE])
        };

        let pool = Self {
            buffers,
            free_list: AtomicU64::new(0),
            total_allocations: AtomicUsize::new(0),
            failed_allocations: AtomicUsize::new(0),
            current_in_use: AtomicUsize::new(0),
            max_in_use: AtomicUsize::new(0),
        };

        // Set all bits to 1 (all buffers are free)
        // We ensure POOL_SIZE <= 64 so this won't overflow
        let mask = (1u64 << POOL_SIZE) - 1;
        pool.free_list.store(mask, Ordering::Relaxed);

        pool
    }

    /// Allocate a buffer from the pool without any heap allocations
    fn allocate(&self) -> Option<MessageBuffer> {
        let mut current = self.free_list.load(Ordering::Relaxed);

        loop {
            // Find the first free buffer (first set bit)
            let index = current.trailing_zeros() as usize;

            // If no free buffers are available
            if index >= POOL_SIZE {
                self.failed_allocations.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            // Calculate the mask for this buffer
            let mask = 1u64 << index;

            // Clear the bit in the free list to mark as used
            let new = current & !mask;

            // Attempt to atomically update the free list
            match self.free_list.compare_exchange_weak(
                current,
                new,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                // Success - we've reserved the buffer
                Ok(_) => {
                    self.total_allocations.fetch_add(1, Ordering::Relaxed);
                    let current_in_use = self.current_in_use.fetch_add(1, Ordering::Relaxed) + 1;

                    // Update max buffers in use
                    let mut max_in_use = self.max_in_use.load(Ordering::Relaxed);
                    while current_in_use > max_in_use {
                        match self.max_in_use.compare_exchange_weak(
                            max_in_use,
                            current_in_use,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(x) => max_in_use = x,
                        }
                    }

                    return Some(MessageBuffer {
                        pool: self,
                        index,
                        length: 0,
                    });
                }
                // Failed, another thread modified the free list
                Err(e) => current = e,
            }
        }
    }

    /// Get current utilization metrics
    fn get_metrics(&self) -> PoolMetrics {
        PoolMetrics {
            pool_size: POOL_SIZE,
            buffer_size: BUFFER_SIZE,
            total_allocations: self.total_allocations.load(Ordering::Relaxed),
            failed_allocations: self.failed_allocations.load(Ordering::Relaxed),
            current_in_use: self.current_in_use.load(Ordering::Relaxed),
            max_in_use: self.max_in_use.load(Ordering::Relaxed),
        }
    }
}

/// Statistics about the buffer pool usage
struct PoolMetrics {
    pool_size: usize,
    buffer_size: usize,
    total_allocations: usize,
    failed_allocations: usize,
    current_in_use: usize,
    max_in_use: usize,
}

impl PoolMetrics {
    /// Print a formatted report of the metrics
    fn print_report(&self, elapsed: Duration) {
        println!("\n=== Zero-Allocation Buffer Pool Metrics ===");
        println!("Pool configuration:");
        println!("  - Buffer size: {} bytes", self.buffer_size);
        println!("  - Pool size: {} buffers", self.pool_size);
        println!(
            "  - Total pre-allocated memory: {} bytes",
            self.pool_size * self.buffer_size
        );

        println!("\nUtilization:");
        println!("  - Total allocations: {}", self.total_allocations);
        println!(
            "  - Failed allocation attempts: {}",
            self.failed_allocations
        );
        println!(
            "  - Maximum buffers in use: {} of {} ({:.1}%)",
            self.max_in_use,
            self.pool_size,
            (self.max_in_use as f64 / self.pool_size as f64) * 100.0
        );
        println!(
            "  - Currently in use: {} of {} ({:.1}%)",
            self.current_in_use,
            self.pool_size,
            (self.current_in_use as f64 / self.pool_size as f64) * 100.0
        );

        println!("\nPerformance:");
        println!("  - Total processing time: {:?}", elapsed);
        println!(
            "  - Average time per message: {:?}",
            if self.total_allocations > 0 {
                elapsed / self.total_allocations as u32
            } else {
                Duration::from_secs(0)
            }
        );
        println!(
            "  - Throughput: {:.2} messages/second",
            self.total_allocations as f64 / elapsed.as_secs_f64()
        );

        if self.failed_allocations > 0 {
            println!(
                "\nWARNING: {} allocation attempts failed due to pool exhaustion",
                self.failed_allocations
            );
            println!("Consider increasing the pool size for your workload.");
        } else {
            println!("\nNo allocation failures - pool size is adequate for the workload.");
            if self.max_in_use < self.pool_size / 2 {
                println!(
                    "NOTE: Pool utilization is low ({:.1}%). Consider reducing pool size to save memory.",
                    (self.max_in_use as f64 / self.pool_size as f64) * 100.0
                );
            }
        }
    }
}

impl<'a> MessageBuffer<'a> {
    /// Get a mutable reference to the underlying buffer
    fn as_mut_slice(&mut self) -> &mut [u8] {
        // Safety: We have exclusive access to this buffer as guaranteed by our free_list
        unsafe { &mut (*self.pool.buffers[self.index].data.get())[..BUFFER_SIZE] }
    }

    /// Set the actual data length in the buffer
    fn set_length(&mut self, length: usize) {
        self.length = length.min(BUFFER_SIZE);
    }

    /// Get a slice of the actual data in the buffer
    fn as_slice(&self) -> &[u8] {
        // Safety: We have exclusive access to this buffer as guaranteed by our free_list
        unsafe { self.pool.buffers[self.index].get(self.length) }
    }

    /// Process this message without any allocations
    fn process(&self) {
        // In a real application, you would do actual processing here
        // For example, parse the message, apply business logic, etc.

        // Simulate processing by calculating a simple checksum
        let mut checksum: u8 = 0;
        for byte in self.as_slice() {
            checksum = checksum.wrapping_add(*byte);
        }

        // In a high-performance system, you might not want to log every message
        if thread_rng_fast() % 100 == 0 {
            // Only log ~1% of messages to reduce output noise
            println!(
                "Processed message of length {} with checksum {}",
                self.length, checksum
            );
        }
    }
}

impl<'a> Drop for MessageBuffer<'a> {
    fn drop(&mut self) {
        // When the buffer is dropped, mark it as free in the pool
        let mask = 1u64 << self.index;
        self.pool.free_list.fetch_or(mask, Ordering::Release);
        self.pool.current_in_use.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<'a> fmt::Debug for MessageBuffer<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MessageBuffer {{ index: {}, length: {} }}",
            self.index, self.length
        )
    }
}

/// A very simple and fast thread-local random number generator
/// This avoids allocations that might happen with the rand crate
fn thread_rng_fast() -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();

    // Hash the current thread ID and time
    std::thread::current().id().hash(&mut hasher);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .subsec_nanos()
        .hash(&mut hasher);

    hasher.finish() as u32
}

/// Simulates a message producer
fn produce_messages(pool: &MessagePool, thread_id: usize, count: usize) {
    let start = Instant::now();
    let mut processed = 0;
    let mut failures = 0;

    println!(
        "Thread {} starting - will process {} messages",
        thread_id, count
    );

    for i in 0..count {
        // Try to get a buffer from the pool
        match pool.allocate() {
            Some(mut buffer) => {
                // Write some data to the buffer
                let data = buffer.as_mut_slice();

                // Fill with some pattern (in a real app this might be from a network socket)
                for j in 0..16 {
                    data[j] = ((thread_id * 1000 + i + j) % 256) as u8;
                }

                // Set the actual length of data
                buffer.set_length(16);

                // Process the message
                buffer.process();
                processed += 1;

                // Buffer will be automatically returned to the pool when dropped

                // Simulate variable processing time
                if thread_rng_fast() % 50 == 0 {
                    thread::sleep(Duration::from_micros(1));
                }
            }
            None => {
                failures += 1;
                // In a real application, you might wait and retry
                thread::sleep(Duration::from_millis(1));
            }
        }
    }

    let elapsed = start.elapsed();
    println!(
        "Thread {} completed - processed {} messages in {:?} ({:.2} msgs/sec), {} failures",
        thread_id,
        processed,
        elapsed,
        processed as f64 / elapsed.as_secs_f64(),
        failures
    );
}

/// Zero allocation example with multi-threaded message processing
fn main() {
    // Create a shared message pool wrapped in Arc for thread safety
    let pool = Arc::new(MessagePool::new());

    println!("=== Zero-Allocation Message Processing Example ===");
    println!(
        "Pool size: {} buffers of {} bytes each",
        POOL_SIZE, BUFFER_SIZE
    );
    println!(
        "Total pre-allocated memory: {} bytes",
        POOL_SIZE * BUFFER_SIZE
    );

    // Create multiple producer threads
    let num_threads = 8;
    let messages_per_thread = 5000;
    let total_messages = num_threads * messages_per_thread;

    println!(
        "\nProcessing {} messages across {} threads...",
        total_messages, num_threads
    );

    let start = Instant::now();

    // Spawn threads
    let mut handles = Vec::with_capacity(num_threads);
    for i in 0..num_threads {
        let pool_clone = Arc::clone(&pool);
        let handle = thread::spawn(move || {
            produce_messages(&pool_clone, i, messages_per_thread);
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();

    // Print detailed metrics
    pool.get_metrics().print_report(elapsed);
}
