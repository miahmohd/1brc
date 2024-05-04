use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::Display,
    fs::File,
    io::Read,
    os::linux::fs::MetadataExt,
    sync::{
        mpsc::{self, Receiver},
        Arc, Mutex,
    },
    thread,
    time::Instant,
};

use anyhow::{bail, Result};

enum Message<T> {
    Done,
    Data(T),
}

#[derive(Debug)]
struct Record {
    min: i32,
    total: i32,
    max: i32,
    count: i32,
}

impl Record {
    fn new(t: i32) -> Self {
        Record {
            min: t,
            total: 0,
            max: t,
            count: 1,
        }
    }

    fn add_temperature(&mut self, t: i32) {
        if t < self.min {
            self.min = t;
        } else if t > self.max {
            self.max = t;
        }

        self.count += 1;
        self.total += t;
    }

    fn merge(&mut self, other: &Record) {
        if other.min < self.min {
            self.min = other.min;
        } else if other.max > self.max {
            self.max = other.max;
        }

        self.count += other.count;
        self.total += other.total;
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:.1}/{:.1}/{:.1}",
            self.min as f32 / 10.0,
            self.total as f32 / 10.0 / self.count as f32,
            self.max as f32 / 10.0,
        )
    }
}

// fn worker_handler(rx: Arc<Mutex<Receiver<Message<Vec<u8>>>>>) -> Result<HashMap<Vec<u8>, Record>> {
//     let mut map: HashMap<Vec<u8>, Record> = HashMap::new();

//     let mut total_waited = 0;
//     loop {
//         let buffer;

//         let now = Instant::now();
//         let rx = rx.lock().unwrap();
//         match rx.recv() {
//             Ok(Message::Data(b)) => buffer = b,
//             Ok(Message::Done) => {
//                 println!("total waited on lock {} micros", total_waited);
//                 return Ok(map);
//             }
//             Err(e) => {
//                 bail!(e)
//             }
//         }
//         drop(rx);
//         total_waited += now.elapsed().as_micros();
//         // Temp has always 1 decimal position, use integer x10
//         let mut temp: i32 = 0;
//         let mut temp_sign = 1;
//         let mut place_buffer = Vec::with_capacity(50);

//         for byte in buffer {
//             // println!("byte {}", byte);
//             match byte {
//                 b'.' => continue,
//                 b';' => {}

//                 b'\n' => {
//                     temp = temp * temp_sign;

//                     if let Some(r) = map.get_mut(&place_buffer) {
//                         r.add_temperature(temp);
//                     } else {
//                         map.insert(place_buffer.clone(), Record::new(temp));
//                     }

//                     place_buffer.clear();
//                     temp = 0;
//                     temp_sign = 1;
//                 }

//                 b'-' => temp_sign = -1,

//                 b'0'..=b'9' => temp = temp * 10 + (byte as i32) - b'0' as i32,

//                 _ => place_buffer.push(byte),
//             }
//         }

//         // sx.send(map)?;
//     }
// }

fn process_chunk(
    f: &File,
    file_size: u64,
    start_offset: u64,
    end_offset: u64,
) -> Result<HashMap<Vec<u8>, Record>> {
    todo!()
}

fn main() -> Result<()> {
    let f = File::open("measurements.txt")?;
    let file_size = f.metadata()?.len();
    let available_parallelism: usize = thread::available_parallelism()?.into();
    let work_size = file_size / (available_parallelism as u64);

    thread::scope(|scope| {
        let f = &f;
        let mut threads = Vec::new();

        println!("file size {} work_size {}", file_size, work_size);

        for i in 0..available_parallelism {
            let start_offset = i as u64 * work_size;

            threads.push(scope.spawn(move || {
                process_chunk(
                    f,
                    file_size,
                    start_offset,
                    file_size.min(start_offset + work_size),
                )
            }));
        }

        let map = threads
            .into_iter()
            .filter_map(|t| t.join().unwrap().ok())
            .fold(HashMap::new(), |mut acc: HashMap<Vec<u8>, Record>, m| {
                for (k, v) in m {
                    if let Some(r) = acc.get_mut(&k) {
                        r.merge(&v);
                    } else {
                        acc.insert(k, v);
                    }
                }

                acc
            });

        let mut pairs = map.into_iter().collect::<Vec<_>>();
        pairs.sort_unstable_by(|a, b| {
            for (a, b) in a.0.iter().zip(b.0.iter()) {
                if a.cmp(b) != Ordering::Equal {
                    return a.cmp(b);
                }
                continue;
            }
            return Ordering::Equal;
        });

        for pair in pairs {
            println!(
                "{}={}",
                unsafe { String::from_utf8_unchecked(pair.0) },
                pair.1
            );
        }
    });

    // let (buffer_sx, buffer_rx) = mpsc::channel::<Message<Vec<u8>>>();
    // let buffer_rx = Arc::new(Mutex::new(buffer_rx));

    // for _ in 0..thread_count {
    //     let rx = buffer_rx.clone();
    //     threads.push(thread::spawn(move || worker_handler(rx)));
    // }

    // let mut buffer = [0; 1024 * 1024];
    // let mut rest: [u8; 100] = [0; 100];
    // let mut rest_len = 0;
    // // let mut support = Vec::with_capacity(1024 * 1024 + 100);

    // loop {
    //     if let Ok(n) = f.read(&mut buffer[rest_len..]) {
    //         buffer[0..rest_len].copy_from_slice(&rest[0..rest_len]);

    //         let n = rest_len + n;
    //         if n == 0 {
    //             // EOF
    //             break;
    //         }

    //         for i in 0..n {
    //             match buffer[n - 1 - i] {
    //                 b'\n' => {
    //                     rest_len = i;
    //                     rest[0..rest_len].copy_from_slice(&buffer[(n - i)..n]);

    //                     buffer_sx
    //                         .send(Message::Data(buffer[..n - 1].to_vec()))
    //                         .unwrap();

    //                     break;
    //                 }
    //                 _ => continue,
    //             }
    //         }
    //     }
    //     // println!("Chuck read in {}ns", now.elapsed().as_nanos());
    // }
    // println!("Done");

    // let now = Instant::now();

    // for _ in 0..thread_count {
    //     buffer_sx.send(Message::Done).unwrap();
    // }

    Ok(())
}
