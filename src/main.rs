use std::{
    collections::HashMap,
    env,
    fmt::Display,
    fs::File,
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
    let available_parallelism: usize = thread::available_parallelism()?.into();
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

    let mut start_offset = if start_offset == 0 {
        start_offset
    } else {
        align_start_line(f, start_offset)
    };

    let end_offset = if end_offset == file_size {
        end_offset
    } else {
        align_start_line(f, end_offset)
    };

    f.read_at(&mut buffer, start_offset).unwrap();

    let mut temp: i32 = 0;
    let mut temp_sign = 1;
    let mut place_buffer = Vec::with_capacity(50);

    while start_offset < end_offset {
        if let Ok(n) = f.read_at(&mut buffer, start_offset) {
            let mut read_len = n;

            // eof
            if read_len == 0 {
                break;
            }

            // process until end_offset
            if start_offset + read_len as u64 > end_offset {
                read_len = (end_offset - start_offset) as usize;
            }

            for byte in &buffer[..read_len] {
                let byte = *byte;
                match byte {
                    b'\n' => {
                        temp = temp * temp_sign;

                        if let Some(r) = map.get_mut(&place_buffer) {
                            r.add_temperature(temp);
                        } else {
                            map.insert(place_buffer.clone(), Record::new(temp));
                        }

                        // Reset startin new line
                        temp = 0;
                        temp_sign = 1;
                        place_buffer.clear();
                    }

                    b'-' => temp_sign = -1,

                    b'0'..=b'9' => temp = temp * 10 + (byte as i32) - b'0' as i32,

                    b'.' => continue,
                    b';' => continue,
                    _ => place_buffer.push(byte),
                }
            }

            start_offset = start_offset + read_len as u64;
        }
    }

    Ok(map)
}

fn align_start_line(f: &File, offset: u64) -> u64 {
    let end_offset = offset;
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

    format!("{{{}}}", s)
}
