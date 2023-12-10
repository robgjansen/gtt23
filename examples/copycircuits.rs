use std::path::PathBuf;

use clap::Parser;
use env_logger::{Builder, Target};
use hdf5::filters::blosc_set_nthreads;
use indicatif::{ProgressBar, ProgressStyle};
use log::{self, LevelFilter};
use ndarray::{s, Array1};

use gtt23::Circuit;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// Copy circuits from an HDF5 dataset into a new HDF5 file
pub struct Cli {
    /// Input path to an HDF5 file from which to copy circuits
    #[arg(value_name = "PATH", required = true)]
    pub input: PathBuf,
    /// Output path to write the HDF5 file containing the copied circuits
    #[arg(
        short,
        long,
        value_name = "PATH",
        default_value = "./gtt23-copied.hdf5"
    )]
    pub output: PathBuf,
}

fn main() -> anyhow::Result<()> {
    blosc_set_nthreads(16);

    Builder::new()
        .target(Target::Stderr)
        .filter_level(LevelFilter::Info)
        .init();

    log::info!(
        "Blosc is available {} num threads {}",
        hdf5::filters::blosc_available(),
        hdf5::filters::blosc_get_nthreads()
    );

    let cli = Cli::parse();

    let in_file = hdf5::File::open(&cli.input)?;
    let in_ds = in_file.dataset("circuits")?;
    let n_tot_circs = in_ds.size();

    log::info!("Found {n_tot_circs} circuits");

    // Make an dataset with the known size.
    let out_file = hdf5::File::create(&cli.output)?;
    let out_ds = out_file
        .new_dataset_builder()
        .chunk(25)
        .blosc_zstd(9, false) // level 9, no shuffle
        .empty::<Circuit>()
        .shape(n_tot_circs)
        .create("circuits")?;

    // Track progress.
    let pb = pb_new(n_tot_circs, format!("Copying circuits"));
    pb.tick();

    // Write in chunks for better progress info.
    let mut tot_written = 0;
    let step = 1_000;

    for wr_begin in (0..n_tot_circs).step_by(step) {
        let wr_end = std::cmp::min(wr_begin + step, n_tot_circs);

        let circ_array: Array1<Circuit> = in_ds.read_slice(s![wr_begin..wr_end])?;
        out_ds.write_slice(&circ_array, s![wr_begin..wr_end])?;

        let wrote = wr_end - wr_begin;
        tot_written += wrote;
        pb.inc(wrote as u64);
    }

    out_file.close()?;

    // Sanity check
    if tot_written != n_tot_circs {
        log::warn!("Only wrote {tot_written}/{n_tot_circs} circuits");
    }

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
