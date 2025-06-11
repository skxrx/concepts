use std::fmt::Debug;
use std::ptr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Node<T: Debug> {
    id: usize,
    data: T,
    next: AtomicPtr<Node<T>>,
}

impl<T: Debug> Node<T> {
    fn new(id: usize, data: T) -> Self {
        Node {
            id,
            data,
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

#[derive(Debug)]
struct LockFreeQueue<T: Debug> {
    id_counter: AtomicUsize,
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    push_count: AtomicUsize,
    pop_count: AtomicUsize,
    push_time_stats: Mutex<OperationTimeStats>,
    pop_time_stats: Mutex<OperationTimeStats>,
}

#[derive(Debug, Default)]
struct OperationTimeStats {
    total_time: Duration,
    min_time: Option<Duration>,
    max_time: Option<Duration>,
    count: usize,
}

impl OperationTimeStats {
    fn new() -> Self {
        OperationTimeStats {
            total_time: Duration::new(0, 0),
            min_time: None,
            max_time: None,
            count: 0,
        }
    }

    fn update(&mut self, operation_time: Duration) {
        self.total_time += operation_time;
        self.count += 1;

        match self.min_time {
            Some(min) if operation_time < min => self.min_time = Some(operation_time),
            None => self.min_time = Some(operation_time),
            _ => {}
        }

        match self.max_time {
            Some(max) if operation_time > max => self.max_time = Some(operation_time),
            None => self.max_time = Some(operation_time),
            _ => {}
        }
    }

    fn average(&self) -> Option<Duration> {
        if self.count > 0 {
            Some(self.total_time / self.count as u32)
        } else {
            None
        }
    }
}

impl<T: Debug> LockFreeQueue<T> {
    fn new() -> Self {
        let id_counter = AtomicUsize::new(0);
        let dummy_id = id_counter.fetch_add(1, Ordering::Relaxed);
        let dummy = Box::into_raw(Box::new(Node::new(dummy_id, unsafe { std::mem::zeroed() })));

        println!("[Queue] Initialized with dummy node {}", dummy_id);

        LockFreeQueue {
            id_counter,
            head: AtomicPtr::new(dummy),
            tail: AtomicPtr::new(dummy),
            push_count: AtomicUsize::new(0),
            pop_count: AtomicUsize::new(0),
            push_time_stats: Mutex::new(OperationTimeStats::new()),
            pop_time_stats: Mutex::new(OperationTimeStats::new()),
        }
    }

    fn get_next_id(&self) -> usize {
        self.id_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn push(&self, thread_id: usize, value: T) {
        let node_id = self.get_next_id();
        let new_node = Box::into_raw(Box::new(Node::new(node_id, value)));
        println!(
            "[Thread {}] Created node {} with value {:?}",
            thread_id,
            node_id,
            unsafe { &(*new_node).data }
        );

        let start_time = Instant::now();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let tail = self.tail.load(Ordering::Acquire);
            let tail_id = unsafe { (*tail).id };
            let next = unsafe { (*tail).next.load(Ordering::Relaxed) };

            println!(
                "[Thread {}] Attempt {} - Current tail is node {}",
                thread_id, attempts, tail_id
            );

            if tail == self.tail.load(Ordering::Relaxed) {
                if next.is_null() {
                    println!(
                        "[Thread {}] Tail node {} has null next pointer, attempting CAS",
                        thread_id, tail_id
                    );

                    if unsafe {
                        (*tail)
                            .next
                            .compare_exchange(next, new_node, Ordering::Release, Ordering::Relaxed)
                            .is_ok()
                    } {
                        println!(
                            "[Thread {}] Successfully linked node {} to tail",
                            thread_id, node_id
                        );

                        let result = self.tail.compare_exchange(
                            tail,
                            new_node,
                            Ordering::Release,
                            Ordering::Relaxed,
                        );

                        if result.is_ok() {
                            println!(
                                "[Thread {}] Successfully updated tail to node {}",
                                thread_id, node_id
                            );
                        } else {
                            println!(
                                "[Thread {}] Failed to update tail, but node is linked - other thread will help",
                                thread_id
                            );
                        }

                        let elapsed = start_time.elapsed();
                        println!(
                            "[Thread {}] Push operation completed in {:?} after {} attempts",
                            thread_id, elapsed, attempts
                        );

                        if let Ok(mut stats) = self.push_time_stats.lock() {
                            stats.update(elapsed);
                        }

                        self.push_count.fetch_add(1, Ordering::Relaxed);
                        return;
                    } else {
                        println!(
                            "[Thread {}] CAS failed - tail was updated by another thread",
                            thread_id
                        );
                    }
                } else {
                    let next_id = unsafe { (*next).id };
                    println!(
                        "[Thread {}] Tail node {} has next pointer to node {}, helping to update tail",
                        thread_id, tail_id, next_id
                    );

                    let result = self.tail.compare_exchange(
                        tail,
                        next,
                        Ordering::Release,
                        Ordering::Relaxed,
                    );

                    if result.is_ok() {
                        println!(
                            "[Thread {}] Successfully helped update tail to node {}",
                            thread_id, next_id
                        );
                    } else {
                        println!(
                            "[Thread {}] Failed to help update tail - another thread may have updated it",
                            thread_id
                        );
                    }
                }
            } else {
                println!(
                    "[Thread {}] Tail changed during operation, retrying",
                    thread_id
                );
            }

            if attempts % 5 == 0 {
                thread::sleep(Duration::from_millis(1));
            }
        }
    }

    fn pop(&self, thread_id: usize) -> Option<T> {
        let start_time = Instant::now();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let head = self.head.load(Ordering::Acquire);
            let head_id = unsafe { (*head).id };
            let tail = self.tail.load(Ordering::Acquire);
            let tail_id = unsafe { (*tail).id };
            let next = unsafe { (*head).next.load(Ordering::Acquire) };

            println!(
                "[Thread {}] Pop attempt {} - Head is node {}, Tail is node {}",
                thread_id, attempts, head_id, tail_id
            );

            if head == self.head.load(Ordering::Relaxed) {
                if head == tail {
                    if next.is_null() {
                        println!(
                            "[Thread {}] Queue is empty (head=tail={} and next is null)",
                            thread_id, head_id
                        );
                        return None;
                    }

                    let next_id = unsafe { (*next).id };
                    println!(
                        "[Thread {}] Head and tail both point to node {}, but next is node {}, helping update tail",
                        thread_id, head_id, next_id
                    );

                    let result = self.tail.compare_exchange(
                        tail,
                        next,
                        Ordering::Release,
                        Ordering::Relaxed,
                    );

                    if result.is_ok() {
                        println!(
                            "[Thread {}] Successfully helped update tail to node {}",
                            thread_id, next_id
                        );
                    } else {
                        println!(
                            "[Thread {}] Failed to help update tail - another thread may have updated it",
                            thread_id
                        );
                    }
                } else {
                    let next_id = unsafe { (*next).id };
                    let data = unsafe { ptr::read(&(*next).data) };
                    println!(
                        "[Thread {}] Found value {:?} at node {}, attempting to update head",
                        thread_id, data, next_id
                    );

                    if self
                        .head
                        .compare_exchange(head, next, Ordering::Release, Ordering::Relaxed)
                        .is_ok()
                    {
                        println!(
                            "[Thread {}] Successfully updated head to node {}, removing node {}",
                            thread_id, next_id, head_id
                        );
                        unsafe {
                            let _ = Box::from_raw(head);
                        }

                        let elapsed = start_time.elapsed();
                        println!(
                            "[Thread {}] Pop operation completed in {:?} after {} attempts",
                            thread_id, elapsed, attempts
                        );

                        if let Ok(mut stats) = self.pop_time_stats.lock() {
                            stats.update(elapsed);
                        }

                        self.pop_count.fetch_add(1, Ordering::Relaxed);
                        return Some(data);
                    } else {
                        println!(
                            "[Thread {}] Failed to update head - another thread may have updated it",
                            thread_id
                        );
                    }
                }
            } else {
                println!(
                    "[Thread {}] Head changed during operation, retrying",
                    thread_id
                );
            }

            if attempts % 5 == 0 {
                thread::sleep(Duration::from_millis(1));
            }
        }
    }

    fn get_stats(&self) -> (usize, usize) {
        (
            self.push_count.load(Ordering::Relaxed),
            self.pop_count.load(Ordering::Relaxed),
        )
    }

    fn get_time_stats(&self) -> (OperationTimeStats, OperationTimeStats) {
        let push_stats = self.push_time_stats.lock().unwrap().clone();
        let pop_stats = self.pop_time_stats.lock().unwrap().clone();
        (push_stats, pop_stats)
    }
}

impl Clone for OperationTimeStats {
    fn clone(&self) -> Self {
        OperationTimeStats {
            total_time: self.total_time,
            min_time: self.min_time,
            max_time: self.max_time,
            count: self.count,
        }
    }
}

impl<T: Debug> Drop for LockFreeQueue<T> {
    fn drop(&mut self) {
        println!("[Queue] Dropping queue and freeing memory");
        while let Some(_) = self.pop(999) {}

        let dummy = self.head.load(Ordering::Relaxed);
        if !dummy.is_null() {
            println!("[Queue] Freeing dummy node");
            unsafe {
                let _ = Box::from_raw(dummy);
            }
        }
    }
}

fn main() {
    println!("=== Lock-Free Queue Demonstration ===");
    let queue = Arc::new(LockFreeQueue::<i32>::new());

    let producer_count = 50;
    let consumer_count = 2;
    let items_per_producer = 10;
    let producer_delay_ms = 5;
    let consumer_delay_ms = 2;

    println!(
        "Configuration: {} producers, {} consumers, {} items per producer",
        producer_count, consumer_count, items_per_producer
    );

    let total_expected_items = producer_count * items_per_producer;
    println!("Expected total items: {}", total_expected_items);

    let simulation_start = Instant::now();

    let mut producer_handles = Vec::new();
    for p_id in 0..producer_count {
        let queue_clone = Arc::clone(&queue);
        let thread_id = p_id + 1;
        let producer = thread::spawn(move || {
            let thread_start = Instant::now();
            println!("[Thread {}] Producer started", thread_id);
            for i in 0..items_per_producer {
                let value = ((thread_id * 100) + i) as i32;
                println!("[Thread {}] Pushing value {}", thread_id, value);
                queue_clone.push(thread_id, value);
                thread::sleep(Duration::from_millis(producer_delay_ms));
            }
            let thread_elapsed = thread_start.elapsed();
            println!(
                "[Thread {}] Producer finished in {:?}",
                thread_id, thread_elapsed
            );
            thread_elapsed
        });
        producer_handles.push(producer);
    }

    let mut consumer_handles = Vec::new();
    for c_id in 0..consumer_count {
        let queue_clone = Arc::clone(&queue);
        let thread_id = producer_count + c_id + 1;
        let consumer = thread::spawn(move || {
            let thread_start = Instant::now();
            println!("[Thread {}] Consumer started", thread_id);
            let mut items_consumed = 0;
            let mut consecutive_empty = 0;

            while items_consumed < total_expected_items && consecutive_empty < 10 {
                match queue_clone.pop(thread_id) {
                    Some(value) => {
                        println!("[Thread {}] Consumed value {}", thread_id, value);
                        items_consumed += 1;
                        consecutive_empty = 0;
                    }
                    None => {
                        println!("[Thread {}] Found empty queue, waiting...", thread_id);
                        consecutive_empty += 1;
                    }
                }
                thread::sleep(Duration::from_millis(consumer_delay_ms));
            }
            let thread_elapsed = thread_start.elapsed();
            println!(
                "[Thread {}] Consumer finished in {:?}, consumed {} items",
                thread_id, thread_elapsed, items_consumed
            );
            (thread_elapsed, items_consumed)
        });
        consumer_handles.push(consumer);
    }

    println!("\n=== Producer Performance ===");
    let mut total_producer_time = Duration::new(0, 0);
    let mut max_producer_time = Duration::new(0, 0);

    for (i, handle) in producer_handles.into_iter().enumerate() {
        let thread_time = handle.join().unwrap();
        let thread_id = i + 1;
        println!("Producer {} completed in {:?}", thread_id, thread_time);

        total_producer_time += thread_time;
        if thread_time > max_producer_time {
            max_producer_time = thread_time;
        }
    }

    let avg_producer_time = total_producer_time / producer_count as u32;
    println!("Average producer time: {:?}", avg_producer_time);
    println!("Maximum producer time: {:?}", max_producer_time);

    println!("\n=== Consumer Performance ===");
    let mut total_consumer_time = Duration::new(0, 0);
    let mut max_consumer_time = Duration::new(0, 0);
    let mut total_items_consumed = 0;

    for (i, handle) in consumer_handles.into_iter().enumerate() {
        let (thread_time, items) = handle.join().unwrap();
        let thread_id = producer_count + i + 1;
        println!(
            "Consumer {} completed in {:?}, consumed {} items",
            thread_id, thread_time, items
        );

        total_consumer_time += thread_time;
        total_items_consumed += items;
        if thread_time > max_consumer_time {
            max_consumer_time = thread_time;
        }
    }

    let avg_consumer_time = total_consumer_time / consumer_count as u32;
    println!("Average consumer time: {:?}", avg_consumer_time);
    println!("Maximum consumer time: {:?}", max_consumer_time);
    println!("Total items consumed: {}", total_items_consumed);

    println!("\n=== Final Queue State ===");
    let mut remaining_items = 0;
    println!("Checking for remaining items:");
    while let Some(value) = queue.pop(999) {
        println!("  Found remaining item: {}", value);
        remaining_items += 1;
    }
    println!("Total remaining items: {}", remaining_items);

    let (push_count, pop_count) = queue.get_stats();
    println!("\n=== Queue Statistics ===");
    println!("Total items pushed: {}", push_count);
    println!("Total items popped: {}", pop_count);

    let simulation_elapsed = simulation_start.elapsed();
    println!("\n=== Overall Performance ===");
    println!("Total simulation time: {:?}", simulation_elapsed);
    println!(
        "Throughput: {:.2} operations/sec",
        (push_count + pop_count) as f64 / simulation_elapsed.as_secs_f64()
    );

    let (push_stats, pop_stats) = queue.get_time_stats();

    println!("\n=== Operation Time Statistics ===");

    println!("Push operations ({})", push_stats.count);
    if let Some(avg) = push_stats.average() {
        println!("  Average time: {:?}", avg);
    }
    if let Some(min) = push_stats.min_time {
        println!("  Best time:    {:?}", min);
    }
    if let Some(max) = push_stats.max_time {
        println!("  Worst time:   {:?}", max);
    }

    println!("Pop operations ({})", pop_stats.count);
    if let Some(avg) = pop_stats.average() {
        println!("  Average time: {:?}", avg);
    }
    if let Some(min) = pop_stats.min_time {
        println!("  Best time:    {:?}", min);
    }
    if let Some(max) = pop_stats.max_time {
        println!("  Worst time:   {:?}", max);
    }

    let total_ops = push_stats.count + pop_stats.count;
    let total_time = push_stats.total_time + pop_stats.total_time;
    let avg_time = if total_ops > 0 {
        Some(total_time / total_ops as u32)
    } else {
        None
    };

    let min_time = match (push_stats.min_time, pop_stats.min_time) {
        (Some(push_min), Some(pop_min)) => Some(std::cmp::min(push_min, pop_min)),
        (Some(push_min), None) => Some(push_min),
        (None, Some(pop_min)) => Some(pop_min),
        _ => None,
    };

    let max_time = match (push_stats.max_time, pop_stats.max_time) {
        (Some(push_max), Some(pop_max)) => Some(std::cmp::max(push_max, pop_max)),
        (Some(push_max), None) => Some(push_max),
        (None, Some(pop_max)) => Some(pop_max),
        _ => None,
    };

    println!("\nAll operations combined ({})", total_ops);
    if let Some(avg) = avg_time {
        println!("  Average time: {:?}", avg);
    }
    if let Some(min) = min_time {
        println!("  Best time:    {:?}", min);
    }
    if let Some(max) = max_time {
        println!("  Worst time:   {:?}", max);
    }

    println!("=== Demonstration Complete ===");
}
