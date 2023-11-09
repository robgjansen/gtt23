use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{bail, Context};
use clap::Parser;
use env_logger::{Builder, Target};
use hdf5::filters::blosc_set_nthreads;
use itertools::Itertools;
use log::{self, LevelFilter};
use ndarray::{arr0, s, Array1};
use serde_json::Value;
use uuid::Uuid;
use zstd::stream::read::Decoder;

use gtt23::{
    fixedascii_from_str, fixedascii_null, Cell, CellCommand, Circuit, Direction, RelayCommand,
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// Create an HDF5 dataset from GTT23 circuits encoded in jsonl files
pub struct Cli {
    /// Input paths to jsonl files written by Tor
    #[arg(value_name = "PATH", required = true)]
    pub input: Vec<PathBuf>,
    /// Output path to write the HDF5 file
    #[arg(short, long, value_name = "PATH", default_value = "./traces.hdf5")]
    pub output: PathBuf,
    /// Number of compression threads
    #[arg(short, long, value_name = "N", default_value = "16")]
    pub threads: u8,
}

fn main() -> anyhow::Result<()> {
    let main_start = Instant::now();
    Builder::new()
        .target(Target::Stderr)
        .filter_level(LevelFilter::Info)
        .init();

    let cli = Cli::parse();
    blosc_set_nthreads(cli.threads);

    log::info!("Initialized with {} files", cli.input.len());

    // Read all json files to count the circuits.
    let n_tot_circs = count_circuits(&cli.input)?;
    log::info!("Found {n_tot_circs} circuits in {} files", cli.input.len());

    // Make an dataset with the known size.
    let file = hdf5::File::create(cli.output)?;
    let ds = file
        .new_dataset_builder()
        .chunk(25)
        .blosc_zstd(9, false) // level 9, no shuffle
        .empty::<Circuit>()
        .shape(n_tot_circs)
        .create("circuits")?;

    // Load and write circuits into the dataset
    let wr_start = Instant::now();
    let mut wr_cursor = 0;

    // Compute circuit indexes as we write.
    let mut index_day = HashMap::<u8, Vec<u32>>::new();
    let mut index_uuid = HashMap::<String, Vec<u32>>::new();
    let mut index_label = HashMap::<String, Vec<u32>>::new();

    // Process all of the files.
    for path in cli.input.iter() {
        log::info!("Processing {:?}", path);
        let circ_array = load_circuits_from_file(path)?;

        let begin = wr_cursor;
        let end = std::cmp::min(wr_cursor + circ_array.len(), n_tot_circs);

        log::info!("Writing {} circuits...", circ_array.len());
        ds.write_slice(&circ_array, s![begin..end])?;
        wr_cursor += end - begin;

        log::info!("Indexing {} circuits...", circ_array.len());
        for (i, circ) in circ_array.iter().enumerate() {
            let ds_index = (begin + i) as u32;
            index_day.entry(circ.day).or_default().push(ds_index);
            index_uuid
                .entry(circ.uuid.to_string())
                .or_default()
                .push(ds_index);
            index_label
                .entry(circuit_label(&circ)?)
                .or_default()
                .push(ds_index);
        }

        log::info!(
            "{}/{} ({:.02}%) done in {:?}, {}/{} ({:.02}%) remain in est. {:?}",
            end,
            n_tot_circs,
            end as f64 / n_tot_circs as f64 * 100.0,
            wr_start.elapsed(),
            n_tot_circs - end,
            n_tot_circs,
            (n_tot_circs - end) as f64 / n_tot_circs as f64 * 100.0,
            wr_start
                .elapsed()
                .div_f64(end as f64)
                .mul_f64((n_tot_circs - end) as f64),
        );
    }

    const CIRCUITS_NOTE: &str =
        "Circuit data as measured from exit relays in the live Tor network. \
        Further description of the dataset can be found in the research paper \
        'Website Fingerprinting with Genuine Tor Traces' by Rob Jansen, \
        Ryan Wails, and Aaron Johnson. Please cite if you use this dataset.";
    ds.new_attr_builder()
        .with_data(&arr0(fixedascii_from_str::<512>(CIRCUITS_NOTE)?))
        .create("note")?;

    // Now write the index datasets.
    log::info!("Writing indexes...");

    // Write the day-to-circ index.
    let index = file.create_group("/index/day")?;

    for (day, indices) in index_day.into_iter().sorted_by_key(|x| x.0) {
        index
            .new_dataset_builder()
            .with_data(&Array1::from_vec(indices))
            .create(format!("{day}").as_str())?;
    }

    const DAY_NOTE: &str =
        "Provides a cached copy of the indices into the circuits dataset of those \
        circuits that were observed on a given day.";
    index
        .new_attr_builder()
        .with_data(&arr0(fixedascii_from_str::<128>(DAY_NOTE)?))
        .create("note")?;

    // Write the uuid-to-circ index.
    let index = file.create_group("/index/uuid")?;

    for (uuid, indices) in index_uuid.into_iter().sorted_by_key(|x| x.0.clone()) {
        if indices.len() != 1 {
            bail!(
                "Uuid should be unique but we found {} indices",
                indices.len()
            );
        }
        index
            .new_dataset_builder()
            .with_data(&arr0(indices[0]))
            .create(uuid.as_str())?;
    }

    const UUID_NOTE: &str =
        "Provides a cached copy of the indices into the circuits dataset of the \
        circuit with the given uuid.";
    index
        .new_attr_builder()
        .with_data(&arr0(fixedascii_from_str::<128>(UUID_NOTE)?))
        .create("note")?;

    // Write the label-to-circ index.
    let index = file.create_group("/index/label")?;

    for (label, indices) in index_label.into_iter().sorted_by_key(|x| x.0.clone()) {
        // We need the `replace("/", "_")` to maintain the path structure in the hdf5.
        index
            .new_dataset_builder()
            .with_data(&Array1::from_vec(indices))
            .create(label.replace("/", "_").as_str())?;
    }

    const LABEL_NOTE: &str =
        "Provides a cached copy of the indices into the circuits dataset of those \
        circuits that match the given label. The label is the circuit's \
        shortest_private_suffix, or the domain if the shortest_private_suffix \
        is null. The label path is modified to replace '/' with '_'.";
    index
        .new_attr_builder()
        .with_data(&arr0(fixedascii_from_str::<512>(LABEL_NOTE)?))
        .create("note")?;

    file.close()?;
    log::info!("All done in {:?}!", main_start.elapsed());
    Ok(())
}

pub fn circuit_label(circ: &Circuit) -> anyhow::Result<String> {
    if circ.shortest_private_suffix != fixedascii_null::<44>()? {
        Ok(circ.shortest_private_suffix.to_string())
    } else {
        Ok(circ.domain.to_string())
    }
}

fn count_circuits(paths: &Vec<PathBuf>) -> anyhow::Result<usize> {
    let mut n_circuits = 0;
    for p in paths.iter() {
        n_circuits += count_lines(p)?;
    }
    Ok(n_circuits)
}

fn count_lines(path: &PathBuf) -> anyhow::Result<usize> {
    let mut stream = open_input_stream(path)?;

    // Use a single string buffer into which we read each line.
    let mut buffer = String::new();
    let mut count = 0;

    // Only reallocates buffer if the next line does not fit.
    while stream.read_line(&mut buffer).map_or(false, |n| n > 0) {
        count += 1;
        // Reclaim capacity.
        buffer.clear();
    }

    Ok(count)
}

fn load_circuits_from_file(path: &PathBuf) -> anyhow::Result<Array1<Circuit>> {
    let mut stream = open_input_stream(path)?;

    // Use a single string buffer into which we read each line.
    let mut buffer = String::new();
    let mut circuits = Vec::new();

    // Only reallocates buffer if the next line does not fit.
    while stream.read_line(&mut buffer).map_or(false, |n| n > 0) {
        circuits.push(decode_circuit(&buffer)?);
        // Reclaim capacity.
        buffer.clear();
    }

    Ok(Array1::from_vec(circuits))
}

fn decode_circuit(jsonl: &String) -> anyhow::Result<Circuit> {
    let json_s = match jsonl.strip_prefix("650 GWF ") {
        Some(s) => s,
        None => &jsonl[..],
    };

    let mut root_val: Value = serde_json::from_str(json_s)?;
    let root_obj = root_val
        .as_object_mut()
        .context("Unable to convert serde value into object")?;

    let day: u8 = root_obj
        .get("day")
        .context("key 'day' missing")?
        .as_u64()
        .context("day to u64")?
        .try_into()?;

    let domain = root_obj
        .get("domain")
        .context("key 'domain' missing")?
        .as_str()
        .context("domain to str")?;
    let domain = fixedascii_from_str::<44>(domain)?;

    // May be null if domain has only public components
    let shortest_private_suffix = {
        let val = root_obj
            .get("shortest_private_suffix")
            .context("key 'shortest_private_suffix' missing")?;
        if val.is_null() {
            fixedascii_null::<44>()?
        } else {
            let sps = val.as_str().context("shortest_private_suffix to str")?;
            fixedascii_from_str::<44>(sps)?
        }
    };

    let port: u16 = root_obj
        .get("port")
        .context("key 'port' missing")?
        .as_u64()
        .context("day to u64")?
        .try_into()?;

    let cells = root_obj
        .get("cells")
        .context("key 'cells' missing")?
        .as_array()
        .context("cells to array")?;

    // Assigns the circuit a new uuid. The len is the actual number of available
    // cells, but the circuit.cells array is always padded to 5000.
    Ok(Circuit {
        uuid: fixedascii_from_str::<32>(&Uuid::new_v4().simple().to_string()[..])?,
        domain,
        shortest_private_suffix,
        day,
        port,
        len: cells.len().try_into()?,
        cells: decode_cells(cells)?,
    })
}

fn decode_cells(json_cells: &Vec<Value>) -> anyhow::Result<[Cell; 5000]> {
    let mut cells = [Cell::empty(); 5000];

    for (i, json_cell) in json_cells.iter().enumerate() {
        let json_cell = json_cell.as_array().context("cell to array")?;

        if json_cell.len() != 4 {
            bail!("expected 4 cell elements, got {}", json_cell.len());
        }

        cells[i].time = json_cell[0].as_f64().context("time to f64")?;

        cells[i].direction = {
            let net_op = json_cell[1].as_i64().context("net_op to i64")?;
            match net_op {
                // relay received cell from client
                0 => Direction::CLIENT_TO_SERVER,
                // relay sent cell toward client
                1 => Direction::SERVER_TO_CLIENT,
                // should never be returned from Tor
                _ => bail!("unexpected net_op {net_op}"),
            }
        };

        cells[i].cell_cmd = {
            let cmd: u8 = json_cell[2]
                .as_u64()
                .context("cell_cmd to u64")?
                .try_into()?;
            match CellCommand::try_from(cmd) {
                Ok(c) => c,
                Err(s) => bail!("{s}"),
            }
        };

        cells[i].relay_cmd = {
            let cmd: u8 = json_cell[3]
                .as_u64()
                .context("relay_cmd to u64")?
                .try_into()?;
            match RelayCommand::try_from(cmd) {
                Ok(c) => c,
                Err(s) => bail!("{s}"),
            }
        };
    }

    Ok(cells)
}

fn open_input_stream(path: &PathBuf) -> anyhow::Result<Box<dyn BufRead>> {
    // Open the file in read-only mode with buffer.
    let file = std::fs::File::open(path)?;

    // Check if we have a zstd-compressed file.
    let use_zstd = if let Some(ext) = path.extension() {
        ext == "zst"
    } else {
        false
    };

    // Run an inline zstd::Decoder if the file is compressed.
    let data_stream: Box<dyn BufRead> = if use_zstd {
        Box::new(BufReader::new(Decoder::new(file)?))
    } else {
        Box::new(BufReader::new(file))
    };

    Ok(data_stream)
}
