use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::bail;
use clap::Parser;
use env_logger::{Builder, Target};
use hdf5::{filters::blosc_set_nthreads, types::FixedAscii};
use indicatif::{ProgressBar, ProgressStyle};
use log::{self, LevelFilter};
use ndarray::{arr0, s, Array1};

use gtt23::{fixedascii_from_str, fixedascii_null, Circuit};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// Create an HDF5 dataset from GTT23 circuits encoded in jsonl files
pub struct Cli {
    /// Input paths to old hdf5 file
    #[arg(value_name = "PATH", required = true)]
    pub input: PathBuf,
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

    let infile = hdf5::File::open(cli.input)?;
    let inds = infile.dataset("circuits")?;
    let n_tot_circs = inds.size();

    log::info!("Found {n_tot_circs} circuits");

    // Make an dataset with the known size.
    let file = hdf5::File::create(&cli.output)?;
    let ds = file
        .new_dataset_builder()
        .chunk(25)
        .blosc_zstd(9, false) // level 9, no shuffle
        .empty::<Circuit>()
        .shape(n_tot_circs)
        .create("circuits")?;

    // Compute circuit indexes as we write.
    let mut index_day = HashMap::<u8, Vec<u32>>::new();
    let mut index_uuid = HashMap::<String, Vec<u32>>::new();
    let mut index_label = HashMap::<String, Vec<u32>>::new();

    // Track progress.
    let pb_main = pb_new(n_tot_circs, format!("Processing circuits"));
    pb_main.tick();

    let fixed_ascii_none = fixedascii_from_str::<44>("None")?;
    let fixed_ascii_null = fixedascii_null::<44>()?;

    // Write in chunks for better progress info.
    let mut tot_written = 0;
    for wr_begin in (0..n_tot_circs).step_by(1_000) {
        let wr_end = std::cmp::min(wr_begin + 1_000, n_tot_circs);

        let mut circ_array: Array1<Circuit> = inds.read_slice(s![wr_begin..wr_end])?;

        for (i, circ) in circ_array.iter_mut().enumerate() {
            let ds_index = (wr_begin + i) as u32;

            if circ.shortest_private_suffix == fixed_ascii_none {
                circ.shortest_private_suffix = fixed_ascii_null;
            }
            let label = circuit_label(&circ, &fixed_ascii_null)?;

            index_day.entry(circ.day).or_default().push(ds_index);
            index_uuid
                .entry(circ.uuid.to_string())
                .or_default()
                .push(ds_index);
            index_label
                .entry(label.to_string())
                .or_default()
                .push(ds_index);
        }

        ds.write_slice(&circ_array, s![wr_begin..wr_end])?;

        let wrote = wr_end - wr_begin;
        tot_written += wrote;
        pb_main.inc(wrote as u64);
    }
    if tot_written != n_tot_circs {
        bail!("Only wrote {tot_written}/{n_tot_circs} circuits");
    }

    const CIRCUITS_NOTE: &str =
        "Circuit data as measured from exit relays in the live Tor network. \
        Further description of the dataset can be found in the research paper \
        'Website Fingerprinting with Genuine Tor Traces' by Rob Jansen, \
        Ryan Wails, and Aaron Johnson. Please cite if you use this dataset.";
    ds.new_attr_builder()
        .with_data(&arr0(fixedascii_from_str::<512>(CIRCUITS_NOTE)?))
        .create("note")?;

    file.close()?;
    infile.close()?;

    // Store the index datasets for now so we can write later.
    serde_json::to_writer(fs::File::create("index_day.json")?, &index_day)?;
    serde_json::to_writer(fs::File::create("index_label.json")?, &index_label)?;
    serde_json::to_writer(fs::File::create("index_uuid.json")?, &index_uuid)?;

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

fn circuit_label(
    circ: &Circuit,
    fixed_ascii_null: &FixedAscii<44>,
) -> anyhow::Result<FixedAscii<44>> {
    if circ.shortest_private_suffix != *fixed_ascii_null {
        Ok(circ.shortest_private_suffix)
    } else {
        Ok(circ.domain)
    }
}
