// #![feature(slice_split_once)]

use std::{
    cmp::Ordering,
    collections::HashMap,
    fs::File,
    io::{BufRead, Read},
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};

#[derive(Debug)]
struct Record {
    min: i32,
    total: i32,
    max: i32,
    count: i32,
}

fn main() -> Result<()> {
    let mut f = File::open("measurements.txt")?;

    // let mut threads = Vec::new();
    // let (buffer_sx, buffer_rx) = mpsc::channel::<Vec<u8>>();
    // let (res_sx, _res_rx) = mpsc::channel::<HashMap<String, Record>>();
    // let buffer_rx = Arc::new(Mutex::new(buffer_rx));

    // for _ in 0..12 {
    //     let rx = Arc::clone(&buffer_rx);
    //     let sx = res_sx.clone();
    //     threads.push(thread::spawn(move || worker_handler(rx, sx)));
    // }
    let mut map: HashMap<Vec<u8>, Record> = HashMap::new();

    let mut buffer: [u8; 1024 * 1024] = [0; 1024 * 1024];
    let mut rest: [u8; 100] = [0; 100];
    let mut rest_len = 0;
    // let mut support = Vec::with_capacity(1024 * 1024 + 100);

    loop {
        // let now = Instant::now();
        if let Ok(n) = f.read(&mut buffer[rest_len..]) {
            buffer[0..rest_len].copy_from_slice(&rest[0..rest_len]);

            let n = rest_len + n;
            if n == 0 {
                // EOF
                break;
            }

            for i in 0..n {
                match buffer[n - 1 - i] {
                    b'\n' => {
                        rest_len = i;
                        rest[0..rest_len].copy_from_slice(&buffer[(n - i)..n]);

                        // Temp has always 1 decimal position, use integer x10
                        let mut temp: i32 = 0;
                        let mut temp_sign = 1;
                        let mut place_buffer = Vec::new();

                        for byte in &buffer[..n - i] {
                            let byte = *byte;
                            // println!("byte {}", byte);
                            match byte {
                                b'.' => continue,
                                b';' => continue,

                                b'\n' => {
                                    temp = temp * temp_sign;

                                    // println!("place {:?}\ntemp {}", place_buffer, temp);

                                    // if let Some(rec) = map.get_mut(k)

                                    // map.entry(place_buffer.clone())
                                    //     .and_modify(|r| {
                                    //         if temp < r.min {
                                    //             r.min = temp;
                                    //         } else if temp > r.max {
                                    //             r.max = temp;
                                    //         }

                                    //         r.count = r.count + 1;
                                    //         r.total = r.total + temp;
                                    //     })
                                    //     .or_insert(Record {
                                    //         min: temp,
                                    //         max: temp,
                                    //         total: 0,
                                    //         count: 1,
                                    //     });

                                    place_buffer.clear();
                                    temp = 0;
                                    temp_sign = 1;
                                }

                                b'-' => temp_sign = -1,

                                b'0'..=b'9' => temp = temp * 10 + (byte as i32) - b'0' as i32,

                                _ => place_buffer.push(byte),
                            }
                        }

                        break;
                    }
                    _ => continue,
                }
            }
        }
        // println!("Chuck read in {}ns", now.elapsed().as_nanos());
    }

    // drop(buffer_sx);

    let now = Instant::now();

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
            "{}={:.1}/{:.1}/{:.1}",
            unsafe { String::from_utf8_unchecked(pair.0) },
            pair.1.min as f32 / 10.0,
            pair.1.total as f32 / 10.0 / pair.1.count as f32,
            pair.1.max as f32 / 10.0,
        );
    }

    // println!("Aggregated in {:.2?} micros", now.elapsed().as_micros());

    Ok(())
}

// fn worker_handler(
//     rx: Arc<Mutex<Receiver<Vec<u8>>>>,
//     sx: Sender<HashMap<String, Record>>,
// ) -> Result<()> {
//     loop {
//         let mut map: HashMap<String, Record> = HashMap::new();
//         let buf;

//         let rx = rx.lock().unwrap();
//         match rx.recv() {
//             Ok(b) => buf = b,
//             Err(e) => {
//                 println!("rec dropped");
//                 bail!(e)
//             }
//         }
//         drop(rx);

//         // let now = Instant::now();
//         // println!("thread {:?} started", std::thread::current().id());
//         for line in buf.lines() {
//             let line = line.unwrap();
//             let (place, rest) = line.split_once(';').unwrap();
//             let temp = rest.parse::<f32>()?;

//             map.entry(place.to_owned())
//                 .and_modify(|r| {
//                     if temp < r.min {
//                         r.min = temp;
//                     } else if temp > r.max {
//                         r.max = temp;
//                     }

//                     r.count = r.count + 1;
//                     r.total = r.total + temp;
//                 })
//                 .or_insert(Record {
//                     min: temp,
//                     max: temp,
//                     total: 0.0,
//                     count: 1,
//                 });
//         }

//         // println!(
//         //     "thread {:?} ended in {}ms",
//         //     std::thread::current().id(),
//         //     now.elapsed().as_millis()
//         // );

//         // sx.send(map)?;
//     }
// }
