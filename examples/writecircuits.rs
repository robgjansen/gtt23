use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

// Must import to prevent SEGFAULTs when writing chunked dataset.
use hdf5::filters::blosc_set_nthreads;

use anyhow::{bail, Context};
use clap::Parser;
use env_logger::{Builder, Target};
use humantime::Timestamp;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{self, LevelFilter};
use ndarray::{self, Array1};
use serde_json::Value;
use uuid::Uuid;
use zstd::stream::read::Decoder;

use gtt23::{self, Cell, CellCommand, Circuit, Direction, RelayCommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// Create an HDF5 dataset from GTT23 circuits encoded in jsonl files
pub struct Cli {
    /// Input paths to jsonl files written by Rob's Tor patch (may be compressed with zstd)
    #[arg(value_name = "PATH", required = true)]
    pub input: Vec<PathBuf>,
    /// Output path to write the HDF5 file
    #[arg(short, long, value_name = "PATH", default_value = "./traces.hdf5")]
    pub output: PathBuf,
    /// Ignore circuits that occurred before this time (e.g., yyyy-mm-ddT00:00:00Z)
    #[arg(short, long, value_name = "TIMESTAMP")]
    pub begin: Option<Timestamp>,
    /// Ignore circuits that occurred after this time (e.g., yyyy-mm-ddT23:59:59Z)
    #[arg(short, long, value_name = "TIMESTAMP")]
    pub end: Option<Timestamp>,
}

fn main() -> anyhow::Result<()> {
    blosc_set_nthreads(4);

    let main_start = Instant::now();
    Builder::new()
        .target(Target::Stderr)
        .filter_level(LevelFilter::Info)
        .init();

    let cli = Cli::parse();
    let begin = match cli.begin {
        Some(t) => Some(t.duration_since(SystemTime::UNIX_EPOCH)?),
        None => None,
    };
    let end = match cli.end {
        Some(t) => Some(t.duration_since(SystemTime::UNIX_EPOCH)?),
        None => None,
    };

    log::info!("Initialized with {} files", cli.input.len());

    // Read all json files to count the circuits.
    let circ_counts = count_circuits(&cli.input)?;
    let n_tot_circs = circ_counts.iter().sum();

    log::info!("Found {n_tot_circs} circuits in {} files", cli.input.len());

    // Make an dataset with the known size.
    let file = hdf5::File::create(cli.output)?;
    let ds = file
        .new_dataset_builder()
        .chunk(25)
        .blosc_zstd(9, false) // level 9, no shuffle
        .empty::<Circuit>()
        .shape(n_tot_circs)
        .create("/circuits")?;

    // Load and write circuits into the dataset
    let mut wr_cursor = 0;

    // Track progress.
    let mpb = MultiProgress::new();
    let pb_main = mpb.add(pb_new(n_tot_circs, format!("Processing circuits")));
    pb_main.tick();

    // Process all of the files.
    for (i, path) in cli.input.iter().enumerate() {
        let name = path_to_name(path);

        // Decode circuits.
        let pb_decode = mpb.add(pb_new(circ_counts[i], format!("Decoding ({name})")));
        let circuits = decode_file(path, &begin, &end, &pb_decode)?;
        pb_decode.finish_and_clear();

        // Write in chunks for better progress info.
        let pb_write = mpb.add(pb_new(circuits.len(), format!("Writing ({name})")));
        pb_write.tick();
        let mut tot_written = 0;

        for begin in (0..circuits.len()).step_by(1_000) {
            let end = std::cmp::min(begin + 1_000, circuits.len());
            let wr_begin = wr_cursor + begin;
            let wr_end = wr_cursor + end;

            ds.write_slice(
                &circuits.slice(ndarray::s![begin..end]),
                ndarray::s![wr_begin..wr_end],
            )?;
            let wrote = wr_end - wr_begin;
            tot_written += wrote;
            pb_write.inc(wrote as u64);
        }

        if tot_written != circuits.len() {
            bail!("Only wrote {tot_written}/{} circuits", circuits.len());
        }

        pb_write.finish_and_clear();
        pb_main.inc(circuits.len() as u64);
        wr_cursor += circuits.len();
    }

    // Since we may have ignored some circuits, snap the dataset down to the actual size.
    if wr_cursor < n_tot_circs {
        log::info!("Resizing dataset from {n_tot_circs} to {wr_cursor} circuits");
        ds.resize(wr_cursor)?;
    }

    file.close()?;
    pb_main.finish();

    log::info!("All done in {:?}!", main_start.elapsed());
    Ok(())
}

fn pb_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{msg}: {wide_bar:.green} {pos}/{len} ({percent}%) [{elapsed_precise} (eta {eta_precise})]",
    )
    .unwrap_or(ProgressStyle::default_bar())
}

fn pb_new(count: usize, message: String) -> ProgressBar {
    ProgressBar::new(count as u64)
        .with_message(message)
        .with_style(pb_style())
}

fn count_circuits(paths: &Vec<PathBuf>) -> anyhow::Result<Vec<usize>> {
    let prog = ProgressBar::new(paths.len() as u64).with_style(pb_style());

    let mut counts = Vec::new();
    for p in paths.iter() {
        prog.set_message(path_to_name(p));
        counts.push(count_lines(p)?);
        prog.inc(1);
    }

    Ok(counts)
}

fn path_to_name(path: &PathBuf) -> String {
    path.file_name()
        .map_or(String::from("unknown"), |s| s.to_string_lossy().to_string())
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

fn decode_file(
    path: &PathBuf,
    begin: &Option<Duration>,
    end: &Option<Duration>,
    pb: &ProgressBar,
) -> anyhow::Result<Array1<Circuit>> {
    let mut stream = open_input_stream(path)?;

    // Use a single string buffer into which we read each line.
    let mut buffer = String::new();
    let mut circuits = Vec::new();

    // Only reallocates buffer if the next line does not fit.
    while stream.read_line(&mut buffer).map_or(false, |n| n > 0) {
        if let Some(circuit) = decode_circuit(&buffer, begin, end)? {
            circuits.push(circuit);
        }
        // Reclaim capacity.
        buffer.clear();
        pb.inc(1);
    }

    Ok(Array1::from_vec(circuits))
}

fn decode_circuit(
    jsonl: &String,
    begin: &Option<Duration>,
    end: &Option<Duration>,
) -> anyhow::Result<Option<Circuit>> {
    let json_s = match jsonl.strip_prefix("650 GWF ") {
        Some(s) => s,
        None => &jsonl[..],
    };

    // Ignore empty lines and \n.
    if json_s.len() <= 1 {
        return Ok(None);
    }

    let mut root_val: Value = serde_json::from_str(json_s).context("Parsing jsonl root")?;
    let root_obj = root_val
        .as_object_mut()
        .context("Unable to convert serde value into object")?;

    let time_created: f64 = root_obj
        .get("time_created")
        .context("key 'time_created' missing")?
        .as_f64()
        .context("time_created to f64")?;

    // Ignore the circuit if not in the requested time interval.
    let created = Duration::from_secs_f64(time_created);
    if begin.is_some_and(|t| created < t) || end.is_some_and(|t| created > t) {
        return Ok(None);
    }

    let day: u8 = match begin {
        Some(t) => (created
            .saturating_sub(*t)
            .as_secs()
            .saturating_div(3600 * 24)
            + 1)
        .try_into()?,
        None => 0,
    };

    let domain = root_obj
        .get("domain")
        .context("key 'domain' missing")?
        .as_str()
        .context("domain to str")?;
    let domain = gtt23::fixedascii_from_str::<44>(domain)?;

    // May be null if domain has only public components
    let shortest_private_suffix = {
        let val = root_obj
            .get("shortest_private_suffix")
            .context("key 'shortest_private_suffix' missing")?;
        if val.is_null() {
            gtt23::fixedascii_null::<44>()?
        } else {
            let sps = val.as_str().context("shortest_private_suffix to str")?;
            gtt23::fixedascii_from_str::<44>(sps)?
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
    Ok(Some(Circuit {
        uuid: gtt23::fixedascii_from_str::<32>(&Uuid::new_v4().simple().to_string()[..])?,
        domain,
        shortest_private_suffix,
        day,
        port,
        len: cells.len().try_into()?,
        cells: decode_cells(cells)?,
    }))
}

fn decode_cells(json_cells: &Vec<Value>) -> anyhow::Result<[Cell; 5000]> {
    let mut cells = [Cell::empty(); 5000];

    for (i, json_cell) in json_cells.iter().enumerate() {
        let json_cell = json_cell.as_array().context("cell to array")?;

        // Each cell is of the form:
        //   [timestamp, side, net_op, cell_cmd, relay_cmd]
        // The side is always 1 which means client-side, so just ignore it.
        if json_cell.len() != 5 {
            bail!("expected 5 cell elements, got {}", json_cell.len());
        }

        cells[i].time = json_cell[0].as_f64().context("time to f64")?;

        cells[i].direction = {
            let net_op = json_cell[2].as_i64().context("net_op to i64")?;
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
            let cmd: u8 = json_cell[3]
                .as_u64()
                .context("cell_cmd to u64")?
                .try_into()?;
            match CellCommand::try_from(cmd) {
                Ok(c) => c,
                Err(s) => bail!("{s}"),
            }
        };

        cells[i].relay_cmd = {
            let cmd: u8 = json_cell[4]
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
