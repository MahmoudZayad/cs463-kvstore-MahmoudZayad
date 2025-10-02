// server.rs
// TCP server that loads TreeMap from disk, persists after mutations.
use clap::{ArgAction, Parser};
use std::net::{TcpListener, TcpStream};
use std::io::{BufRead, BufReader, Write, Seek};
use std::sync::{Arc, RwLock};
use kvstore::TreeMap;
use std::fs::*;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// IP address and port to bind the server socket to
    #[arg(short, long, default_value = "127.0.0.1:4000")]
    addr: String,
    
    /// Do not persist the database contents to disk
    #[arg(short, long, default_value = "false")]
    memonly: bool,

    /// Run the server single-threaded
    #[arg(short, long, default_value_t = true, action = ArgAction::Set)]
    singlethread: bool,

    /// Location of the database file
    #[arg(short, long, default_value = "kvstore.db")]
    dbfile: String,

    /// Location of the database transaction log
    #[arg(short, long, default_value = "/tmp/kvstore_mzayad2.log")]
    logfile: String,

    /// Snapshot interval
    #[arg(short, long, default_value = "1000")]
    snapshot_interval: u64,

    /// Pass this code to the server EXIT command to have it exit
    #[arg(short, long, default_value = "")]
    exit_code: String,    
}

fn handle_client(args: Arc<Args>, stream: TcpStream, map: Arc<RwLock<TreeMap<String, String>>>, log: Arc<RwLock<File>>, batch_counter: Arc<RwLock<u64>>) {
    let mut writer = stream.try_clone().unwrap();
    let reader = BufReader::new(&stream);
    let mut lines = reader.lines();
    let mut response = String::new();
    let mut log_entries = Vec::new();
    while let Some(Ok(line)) = lines.next() {
        let parts: Vec<&str> = line.trim_end().splitn(3, ' ').collect();
        match parts[0] {
            "GET" if parts.len() == 2 => {
                let map = map.read().unwrap();               
                response.push_str(&match map.get(&parts[1].to_string()) {
                    Some(v) => format!("OK {}\r\n", v),
                    None    => "ERR NotFound\r\n".into(),
                });
            }
            "SET" if parts.len() == 3 => {
                let mut map = map.write().unwrap();
                log_entries.push(line.clone());
                map.insert(parts[1].to_string(), parts[2].to_string());
                response.push_str("OK\r\n");
            }
            "REMOVE" if parts.len() == 2 => {
                let mut map = map.write().unwrap();
                response.push_str(match map.remove(&parts[1].to_string()) {
                    Some(_) => {
                        log_entries.push(line.clone());
                        "OK\r\n"
                    }
                    None => "ERR NotFound\r\n",
                });
            }
            "SEEK" if parts.len() == 2 => {
                let map = map.read().unwrap();
                response.push_str(&match map.seek_ge(&parts[1].to_string()) {
                    Some((k, v)) => format!("OK {} {}\r\n", k, v),
                    None          => "ERR NotFound\r\n".into(),
                });
            }
            "ENDBATCH" => {
                // Send responses first
                if response.len() > 0 {
                    writer.write_all(response.as_bytes()).unwrap();
                }
                response = String::new();

                // Handle logging and snapshots
                if log_entries.len() > 0 && !args.memonly {
                    // Write to log
                    let mut log_file = log.write().unwrap();
                    let batch_data = log_entries.join("\n") + "\n";
                    log_file.write_all(batch_data.as_bytes()).unwrap();
                    log_file.flush().unwrap();

                    // Check for snapshot
                    let mut counter = batch_counter.write().unwrap();
                    *counter += 1;
                    if *counter >= args.snapshot_interval {
                        *counter = 0;
                        drop(log_file); // Release lock before snapshot
                        
                        // Create snapshot
                        let map_read = map.read().unwrap();
                        map_read.save_to_file(&args.dbfile).unwrap();
                        drop(map_read);
                        
                        // Truncate log
                        let mut log_file = log.write().unwrap();
                        Seek::rewind(&mut *log_file).unwrap();
                        log_file.set_len(0).unwrap();
                    }
                    
                    log_entries.clear();
                }
            }
            "EXIT" if parts.len() == 2 && parts[1] == args.exit_code  => {
                eprintln!("Received EXIT command with correct exit code. Exiting.");
                std::process::exit(0);
            }
            _ => { 
                response="ERR UnknownCommand\r\n".into();
            }
            // This is a handy special command to help with profiling the server. Would 
            // not recommend having a command like this in your typical key-value store!
        };
    }
}

fn recover_from_log(map: &mut TreeMap<String,String>, log: File) { 
    let mut lines = BufReader::new(log).lines();
    println!("Recovering from log...");
    let mut count = 0;
    while let Some(Ok(line)) = lines.next() {
        count+=1;
        let parts: Vec<&str> = line.trim_end().splitn(3, ' ').collect();
        match parts[0] {
        "SET" if parts.len() == 3 => {
            map.insert(parts[1].to_string(), parts[2].to_string());
        },
        "REMOVE" if parts.len() == 2 => {
            map.remove(&parts[1].to_string());
        },
        _ => { panic!("Bad log entry."); }
        }  
    }
    println!("Recovered {count} updates from log.\n");
}
fn main() -> std::io::Result<()> {
    let args = Arc::new(Args::parse());
    
    let mut map = match TreeMap::load_from_file(&args.dbfile) {
        Ok(m) => m,
        Err(_) => TreeMap::new(),
    };

    {   // Create or open pidfile and write PID to it
        let mut file = std::fs::File::create("server_pid.txt").unwrap();
        writeln!(file, "{}", std::process::id())?;
    }

    if let Ok(true) = std::fs::exists(args.logfile.as_str()) {
        recover_from_log(&mut map, File::open(args.logfile.as_str()).unwrap());
    }    

    let map = Arc::new(RwLock::new(map));
    let batch_counter = Arc::new(RwLock::new(0u64));

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .append(true)
        .open(&args.logfile)?;
    let log = Arc::new(RwLock::new(log_file));

    let listener = TcpListener::bind(&args.addr)?;
    println!("Server listening on {}",args.addr);

    for stream in listener.incoming() {

        match stream {
            Ok(s) => {
                // Nagle's algorithm waits a little bit before acknowledging a received packet. 
                // This is usually a good idea, but not if your program sends small packets and cares about low latency.
                s.set_nodelay(true)?;                 
                let args = args.clone();
                let map = map.clone();
                let log = log.clone();
                let batch_counter = batch_counter.clone();

                // use the new --singlethread command line argument to set this
                if args.singlethread {
                    handle_client(args, s, map, log, batch_counter);
                }
                else { 
                    std::thread::spawn(move || { handle_client(args, s, map, log, batch_counter); });
                }
            }
            Err(e) => eprintln!("Connection failed: {}", e),
        }
    }
    Ok(())
}
