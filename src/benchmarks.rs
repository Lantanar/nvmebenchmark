use crate::util::{construct_allocation_from_distribution, construct_random_allocations, create_random_data, get_random_safe_start, print_2x2, threadsafe_io_batch_complete_64, IoLog, QueuePairError, ONE_GIB};
use rand_distr::Zipf;
use vroom::{memory::{Dma, DmaSlice}, queues, NvmeDevice, NvmeQueuePair, HUGE_PAGE_SIZE};  
use rand::{rngs::SmallRng, Rng, RngCore, SeedableRng};
use std::{cmp::{max, min}, io, sync::{Arc, Mutex}, time::Instant};


pub fn determine_cache_size(mut nvme: NvmeDevice, max_io: u64, single_io_size: u64, write: bool, queue_depth: usize, num_threads: usize) -> (NvmeDevice, Vec<Vec<IoLog>>) {
    let mut max_write = if max_io == 0 {
        ONE_GIB * 8
    } else {
        max_io
    };

    let ns = nvme.namespaces.get(&1).unwrap();
    let max_blocks = ns.blocks;
    let ns_id = ns.id;
    let block_size = ns.block_size;

    let batch_size = queue_depth;

    //needs to be a multiple of block_size, maximum size: HUGE_PAGE_SIZE-1
    let io_size = min(single_io_size, HUGE_PAGE_SIZE as u64 - block_size);
    let io_size  = io_size - (io_size % block_size);
    //approximate amount of loops per results save / atleast so many loops need to be completed once before it is allowed to stop
    let step_size = max(io_size / 8192, 1) * 32;

    if max_write >= max_blocks * block_size {
        max_write = max_blocks * block_size / 2;
    }

    let mut handles = Vec::with_capacity(num_threads);

    let mut queues = Vec::new();

    for _ in 0..num_threads {
        //minimum queue size to not run into issues with a single submit command with a size of 2 MiB
        queues.push(nvme.create_io_queue_pair(max(queue_depth*2,512)).unwrap());
    }

    let queues = Arc::new(Mutex::new(queues));
    

    for i in 0..num_threads {
        let shared_queues = queues.clone();

        let handle = std::thread::spawn(move || {
            let mut results = Vec::new();
            let mut guard = shared_queues.lock().unwrap();
            let mut queue_pair = guard.pop().unwrap();
            drop(guard);

            let dma = create_random_data(io_size as usize);

            //let mut rng = SmallRng::seed_from_u64(Instant::now().elapsed().as_millis() as u64);

            let mut it_check = 0;
            let mut total = 0;
            let mut cumulative_actions: usize = 0;
            let mut t = 0;
            /*let mut last_t = 0;
            let mut dropoff_flag = false;
            let mut end_counter = 0;
            let mut it_start_dropoff = 1;*/
            //let start_block = rng.next_u64() % (max_blocks - max_write / block_size);
            let start_block = (max_io / block_size) * i as u64;
            
            let mut start = Instant::now();

            while it_check < max_write / io_size {

                let res = queue_pair.submit_io(ns_id, block_size, &dma.slice(0..io_size as usize), start_block+it_check*(io_size/block_size), write);
                if res == 0 {
                    eprintln!("Request was not queued, results will be inaccurate");
                }

                total += res;

                while let Some(_) = queue_pair.quick_poll() {
                    total -= 1;
                    cumulative_actions += 1;
                }

                if total >= batch_size {
                    queue_pair.complete_io(total+1-batch_size);
                    cumulative_actions += total+1-batch_size;
                    total -= total+1-batch_size;
                }
                

                if cumulative_actions > step_size as usize {
                    let end = Instant::now();
                    //last_t = t;
                    t = end.duration_since(start).as_micros();

                    /*
                    if !dropoff_flag && (it_check >= step_size*8) && (t * 3 >= last_t * 4) {
                        dropoff_flag = true;
                        it_start_dropoff = it_check / 2;
                    }*/

                    results.push(IoLog {
                        start,
                        end,
                        actions: cumulative_actions,
                        cumulative_size: (cumulative_actions * 8192) as usize,
                    });

                    cumulative_actions = 0;
                    start = Instant::now();
                }
            
                /* 
                if dropoff_flag {
                    end_counter += 1;
                }*/
                
                it_check += 1;
            }
            if total > 0 {
                queue_pair.complete_io(total);
            }
            return (results, queue_pair);
        });
        handles.push(handle);
    }

    eprintln!("{:?}", &handles);

    let mut results = Vec::new();
    for handle in handles {
        let (res, queue_pair) = handle.join().unwrap();
        nvme.delete_io_queue_pair(queue_pair);
        results.push(res);
    }

    /* 
    if it_check >= step_size*8 {
        println!("Could not detect cache size, tested: {}MiB", it_check * block_size / (1024 * 1024));
    } else {
        println!("Cache size: {} MiB", it_check * block_size / (1024 * 1024));
    }

    let data_points = min(50, results.len());
    let mut condensed = Vec::with_capacity(data_points);

    for i in 0..data_points {
        let start = i * results.len() / data_points;
        let end = (i + 1) * results.len() / data_points;

        let chunk = &results[start..end];
        let chunk_len = chunk.len();

        if chunk_len > 0 {
            let n: usize = chunk.iter().map(|(actions, _)| actions).sum();
            let t: u128 = chunk.iter().map(|(_, time)| time).sum();
            condensed.push((n as u128 * block_size as u128 * 1_000_000) / (t * 1024 * 1024));
        }
    }

    condensed.iter().for_each(|x| println!("{} MiB/s", x));*/

    (nvme, results)
}

pub fn single_lba(mut nvme: NvmeDevice, write: bool) -> NvmeDevice {
    let n_loops = 32;

    let ns = nvme.namespaces.get(&1).unwrap();
    let max_blocks = ns.blocks;
    let ns_id = ns.id;
    let block_size = ns.block_size;
    let dma = create_random_data(HUGE_PAGE_SIZE);

    let mut queue_pair = nvme.create_io_queue_pair(128).unwrap();

    
    let lba = get_random_safe_start(block_size, max_blocks, block_size).unwrap();

    if !write {
        let res = queue_pair.submit_io(ns_id, block_size, &dma.slice(0..block_size as usize), lba, true);
        queue_pair.complete_io(res);
    }

    let mut submitted = 0;
    let start = Instant::now();
    for _ in 0..n_loops * HUGE_PAGE_SIZE as u64 / block_size {
        submitted += queue_pair.submit_io(ns_id, block_size, &dma.slice(0..block_size as usize), lba, write);
        
        if submitted >= 64 {
            queue_pair.complete_io(submitted/2);
            submitted -= submitted / 2;
        }
        
    }
    if submitted > 0 {
        queue_pair.complete_io(submitted);
    }
    let duration = start.elapsed();
    println!("Continous {} same LBA completed with {}MiB/s", if write { "write to" } else { "read from" }, n_loops as u128 * HUGE_PAGE_SIZE as u128 * 1_000_000 / (duration.as_micros() * 1024 * 1024));

    nvme.delete_io_queue_pair(queue_pair);
    
    nvme
}

pub fn full_random_combinations(mut nvme: NvmeDevice) -> NvmeDevice {
    let ns = nvme.namespaces.get(&1).unwrap();
    let max_blocks = ns.blocks;
    let ns_id = ns.id;
    let block_size = ns.block_size;
    let mut dma = create_random_data(HUGE_PAGE_SIZE);

    const NUM_IT: usize = 10;
    let mut total= 0;
    let mut results = Vec::new();

    let mut successfull_it = NUM_IT;

    let mut queue_pair = nvme.create_io_queue_pair(128).unwrap();

    for random_from in [false, true] {
        for random_to in [false, true] {
            let allocations = construct_random_allocations(dma.size, max_blocks, block_size, random_from, random_to);
            total = 0;
            for _ in 0..NUM_IT {
                
                let t = std::time::Instant::now();
                queue_pair = match threadsafe_io_batch_complete_64(queue_pair, ns_id, block_size, (&dma, &allocations), true) {
                    Ok(qp) => qp,
                    Err(e) => {
                        successfull_it -= 1;
                        e.queue_pair
                    }
                };
                let duration = t.elapsed();
                
                total += duration.as_micros();
            }
            results.push((1_000_000 * HUGE_PAGE_SIZE as u128 * successfull_it as u128) / (1024*1024 * total as u128));
        }
    }
    nvme.delete_io_queue_pair(queue_pair);

    print_2x2(&results);

    nvme
}

pub fn zipf_single_action(mut nvme: NvmeDevice, write: bool) -> NvmeDevice {
    let ns = nvme.namespaces.get(&1).unwrap();
    let max_blocks = ns.blocks;
    let ns_id = ns.id;
    let block_size = ns.block_size;
    let dma = create_random_data(HUGE_PAGE_SIZE);

    let mut queue_pair = nvme.create_io_queue_pair(128).unwrap();

    for s in 1..3 {
        for n in [4096, 32768, 262144, 2097152] {
            let start_lba = match get_random_safe_start(n * block_size, max_blocks, block_size) {
                Some(x) => x,
                None => continue,
            };

            let distr = Zipf::new(n as f64, s as f64).unwrap();

            let mut allocations = construct_allocation_from_distribution(n as usize * block_size as usize, dma.size, block_size, distr);
            allocations = allocations.iter_mut().map(|x| {x.lba += start_lba; *x}).collect();

            if s==1 && n==4096 {
                println!("{:?}", allocations);
            }

            let t = std::time::Instant::now();
            queue_pair = match threadsafe_io_batch_complete_64(queue_pair, ns_id, block_size, (&dma, &allocations), write) {
                Ok(qp) => qp,
                Err(e) => {
                    eprintln!("Failed to complete some transactions, report may be innacurate");
                    e.queue_pair
                }
            };
            let d = t.elapsed();

            

            if d.as_micros() == 0 {
                println!("Unexpected error where elapsed time is 0");
                continue
            }

            println!("Zipf({},{}): {}MiB/s",n,s,n as u128 * block_size as u128 * 1_000_000/(d.as_micros() * 1024* 1024));
        }
    }
    nvme.delete_io_queue_pair(queue_pair);
    
    nvme
}