use std::{
    collections::HashMap,
    env,
    fmt::Display,
    fs::File,
    io::BufRead,
    os::unix::fs::FileExt,
    thread::{self, ScopedJoinHandle},
};

use anyhow::Result;

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

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    assert!(
        args.len() == 2,
        "Missing measurements file arg. Run 'cargo run --release -- measurements.txt'"
    );

    let f = File::open(&args[1])?;
    let file_size = f.metadata()?.len();
    // let available_parallelism: usize = thread::available_parallelism()?.into();
    let available_parallelism: usize = 1;
    let work_size = file_size / (available_parallelism as u64);

    thread::scope(|scope| {
        let f = &f;
        let mut threads = Vec::new();

        for i in 0..available_parallelism {
            let start_offset = i as u64 * work_size;
            threads.push(
                thread::Builder::new()
                    .name(format!("t_{}", i))
                    .spawn_scoped(scope, move || {
                        process_chunk(
                            f,
                            file_size,
                            start_offset,
                            file_size.min(start_offset + work_size),
                        )
                    })
                    .unwrap(),
            );
        }

        println!("{}", collect_results(threads));
    });

    Ok(())
}

fn process_chunk(
    f: &File,
    file_size: u64,
    start_offset: u64,
    end_offset: u64,
) -> Result<HashMap<Vec<u8>, Record>> {
    const CHUNK_SIZE: usize = 1024 * 1024;
    let mut map: HashMap<Vec<u8>, Record> = HashMap::new();

    let mut buffer = [0; CHUNK_SIZE];
    let mut rest: [u8; 100] = [0; 100];
    let mut rest_len = 0;

    // println!(
    //     "{:?}: file size {}, start {} end {}",
    //     thread::current().id(),
    //     file_size,
    //     start_offset,
    //     end_offset
    // );

    let mut start_offset = align_start(f, start_offset);
    let end_offset = align_end(f, file_size, end_offset);

    f.read_at(&mut buffer, start_offset).unwrap();

    println!(
        "{:?}: first aligned line {:?}",
        thread::current().name(),
        buffer.lines().next()
    );

    // println!(
    //     "{:?} aligned: file size {}, start {} end {}",
    //     thread::current().name(),
    //     file_size,
    //     start_offset,
    //     end_offset
    // );

    while start_offset < end_offset {
        if let Ok(n) = f.read_at(&mut buffer[rest_len..], start_offset) {
            buffer[0..rest_len].copy_from_slice(&rest[0..rest_len]);

            println!(
                "{:?}: first aligned line {:?}",
                thread::current().name(),
                buffer.lines().next()
            );

            assert!(b'A' <= buffer[0] && buffer[0] <= b'Z');

            let mut read_len = rest_len + n;

            // eof
            if read_len == 0 {
                break;
            }

            // Offset by 1 error maybe
            // if start_offset + read_len as u64 > end_offset {
            //     assert!(
            //         end_offset - start_offset < read_len as u64,
            //         "start_offset {}, read_len {}, end_offset {}",
            //         start_offset,
            //         read_len,
            //         end_offset
            //     );

            //     read_len = (end_offset - start_offset) as usize;
            // }

            for i in 0..read_len {
                match buffer[read_len - 1 - i] {
                    b'\n' => {
                        rest_len = i;
                        rest[0..rest_len].copy_from_slice(&buffer[(read_len - i)..read_len]);

                        let mut temp: i32 = 0;
                        let mut temp_sign = 1;
                        let mut place_buffer = Vec::with_capacity(50);

                        for byte in &buffer[..read_len - i] {
                            let byte = *byte;
                            match byte {
                                b'.' => continue,
                                b';' => {}

                                b'\n' => {
                                    temp = temp * temp_sign;

                                    if let Some(r) = map.get_mut(&place_buffer) {
                                        r.add_temperature(temp);
                                    } else {
                                        map.insert(place_buffer.clone(), Record::new(temp));
                                    }

                                    place_buffer.clear();
                                    temp = 0;
                                    temp_sign = 1;
                                }

                                b'-' => temp_sign = -1,

                                b'0'..=b'9' => temp = temp * 10 + (byte as i32) - b'0' as i32,

                                _ => place_buffer.push(byte),
                            }
                        }

                        start_offset = start_offset + read_len as u64;
                        break;
                    }
                    _ => continue,
                }
            }
        }
    }

    Ok(map)
}

fn align_end(f: &File, file_size: u64, end_offset: u64) -> u64 {
    if end_offset == file_size {
        return end_offset;
    }

    let end_offset = end_offset;
    let mut buf: [u8; 100] = [0; 100];

    while let Ok(n) = f.read_at(&mut buf, end_offset) {
        for i in 0..n {
            if buf[i] == b'\n' {
                return end_offset + i as u64 + 1; // end next to \n
            }
        }
    }

    unreachable!()
}

fn align_start(f: &File, start_offset: u64) -> u64 {
    if start_offset == 0 {
        return start_offset;
    }

    let start_offset = start_offset;
    let mut buf: [u8; 100] = [0; 100];

    while let Ok(n) = f.read_at(&mut buf, start_offset) {
        for i in 0..n {
            if buf[i] == b'\n' {
                return start_offset + i as u64 + 1; // start next to \n
            }
        }
    }

    unreachable!()
}

fn collect_results(threads: Vec<ScopedJoinHandle<Result<HashMap<Vec<u8>, Record>>>>) -> String {
    let map = threads
        .into_iter()
        .map(|t| t.join().unwrap().unwrap())
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
    pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let s = pairs
        .into_iter()
        .map(|pair| {
            format!(
                "{}={}",
                unsafe { String::from_utf8_unchecked(pair.0) },
                pair.1
            )
        })
        .collect::<Vec<String>>()
        .join(", ");

    format!("{{ {} }}", s)
}
