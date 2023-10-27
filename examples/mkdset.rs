use hdf5::{File, Result};
use ndarray::{s, Array0};

use gtt23::Circuit;

fn read_hdf5() -> Result<()> {
    let file =
        File::open("/net/hera/storage/rjansen/research/gtfo/tor_measurement/gtt23/gtt23.hdf5")?; // open for reading
    let ds = file.dataset("circuits")?; // open the dataset

    // let file = File::open("test.hdf5")?; // open for reading
    // let ds = file.dataset("circuits")?; // open the dataset

    //let a: Array1<Circuit> = ds.read_slice(s![2..3])?;
    let a: Array0<Circuit> = ds.read_slice(s![2000000])?;

    for c in a.iter() {
        println!("{:?}", c.uuid.as_str());
    }
    Ok(())
}

fn main() -> Result<()> {
    println!("Hello, world!");
    read_hdf5()?;
    Ok(())
}

// use std::collections::HashMap;
// use std::fs::File;
// use std::io::{BufRead, BufReader};
// use std::path::PathBuf;
// use std::sync::atomic::{AtomicBool, Ordering};
// use std::sync::Arc;
// use std::time::{Duration, Instant, SystemTime};

// use anyhow::{bail, Context};
// use ctrlc;
// use uuid::Uuid;
// use zstd::stream::read::Decoder;

// use crate::retrace::RetraceEntry;

// pub fn load_traces_jsonl(path: &PathBuf) -> anyhow::Result<HashMap<Uuid, RetraceEntry>> {
//     let load_start = Instant::now();

//     // Open the file in read-only mode with buffer.
//     let file = File::open(path).context(format!("Unable to open file at {path:?}"))?;

//     // Check if we have a zst compressed file.
//     let use_zstd = if let Some(ext) = path.extension() {
//         ext == "zst"
//     } else {
//         false
//     };

//     // Run an inline zstd::Decoder if the file is compressed.
//     let data_stream: Box<dyn BufRead> = if use_zstd {
//         Box::new(BufReader::new(Decoder::new(file)?))
//     } else {
//         Box::new(BufReader::new(file))
//     };

//     let mut lines = data_stream.lines();
//     let mut map = HashMap::new();

//     log::info!("Loading traces from {path:?}...");

//     while let Some(line_res) = lines.next() {
//         // Trickle up any error reading the line.
//         let line = line_res?;

//         // Trickle up any error decoding the line.
//         let entry = RetraceEntry::from_json_string(line)?;

//         // Our ids _should_ be globally unique, so panic on duplicates
//         let uid = entry.uuid();
//         log::trace!("Decoded trace uuid={uid}");
//         if map.insert(uid, entry).is_some() {
//             bail!("Found duplicate key for id {uid}");
//         }
//     }

//     // If we had a single dict json file, could read like this:
//     // let map = serde_json::from_reader(reader).context("Error decoding json file")?;

//     log::info!(
//         "Loaded {} traces in {:.3?} from {path:?}",
//         map.len(),
//         load_start.elapsed()
//     );
//     Ok(map)
// }
