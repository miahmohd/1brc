use std::{
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
    min: f32,
    total: f32,
    max: f32,
    count: usize,
}

fn main() -> Result<()> {
    let mut f = File::open("measurements.txt")?;

    let mut threads = Vec::new();
    let (buffer_sx, buffer_rx) = mpsc::channel::<Vec<u8>>();
    let (res_sx, _res_rx) = mpsc::channel::<HashMap<String, Record>>();
    let buffer_rx = Arc::new(Mutex::new(buffer_rx));

    for _ in 0..12 {
        let rx = Arc::clone(&buffer_rx);
        let sx = res_sx.clone();
        threads.push(thread::spawn(move || worker_handler(rx, sx)));
    }

    let mut buffer: [u8; 1024 * 1024] = [0; 1024 * 1024];
    let mut rest: [u8; 100] = [0; 100];
    let mut rest_len = 0;
    let mut support = Vec::with_capacity(1024 * 1024 + 100);

    loop {
        // let now = Instant::now();
        if let Ok(n) = f.read(buffer.as_mut_slice()) {
            if n == 0 {
                break;
            }

            // let newLineIndex = buffer.in

            for i in 0..n {
                match buffer[n - 1 - i] {
                    b'\n' => {
                        support.clear();
                        support.extend_from_slice(&rest[0..rest_len]);
                        support.extend_from_slice(&buffer[0..(n - i)]);
                        // println!("support: {:?}", support);
                        buffer_sx.send(support.clone())?;

                        rest_len = i;
                        rest[0..rest_len].copy_from_slice(&buffer[(n - i)..n]);

                        // println!(
                        //     "n: {:?} \n &buffer[0..(n - i)]: {:?} \n  &rest[0..rest_len]:{:?}",
                        //     n,
                        //     &buffer[0..(n - i)],
                        //     &rest[0..rest_len]
                        // );

                        break;
                    }
                    _ => continue,
                }
            }
        }
        // println!("Chuck read in {}ns", now.elapsed().as_nanos());
    }

    // drop(buffer_sx);

    Ok(())
}

fn worker_handler(
    rx: Arc<Mutex<Receiver<Vec<u8>>>>,
    sx: Sender<HashMap<String, Record>>,
) -> Result<()> {
    loop {
        let mut map: HashMap<String, Record> = HashMap::new();
        let buf;

        let rx = rx.lock().unwrap();
        match rx.recv() {
            Ok(b) => buf = b,
            Err(e) => {
                println!("rec dropped");
                bail!(e)
            }
        }
        drop(rx);

        // let now = Instant::now();
        // println!("thread {:?} started", std::thread::current().id());
        for line in buf.lines() {
            let line = line.unwrap();
            let (place, rest) = line.split_once(';').unwrap();
            let temp = rest.parse::<f32>()?;

            map.entry(place.to_owned())
                .and_modify(|r| {
                    if temp < r.min {
                        r.min = temp;
                    } else if temp > r.max {
                        r.max = temp;
                    }

                    r.count = r.count + 1;
                    r.total = r.total + temp;
                })
                .or_insert(Record {
                    min: temp,
                    max: temp,
                    total: 0.0,
                    count: 1,
                });
        }

        // println!(
        //     "thread {:?} ended in {}ms",
        //     std::thread::current().id(),
        //     now.elapsed().as_millis()
        // );

        // sx.send(map)?;
    }
}
