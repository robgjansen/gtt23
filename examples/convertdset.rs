use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::bail;
use clap::Parser;
use env_logger::{Builder, Target};
use hdf5::filters::blosc_set_nthreads;
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

    // Now write the index datasets.
    write_day_index(&file, index_day)?;
    write_label_index(&file, index_label)?;

    file.close()?;
    infile.close()?;

    // the UUID index takes a bit longer to write.
    fs::copy(&cli.output, "with_uuid_idx.hdf5")?;
    let file = hdf5::File::open_rw("with_uuid_idx.hdf5")?;
    write_uuid_index(&file, index_uuid)?;
    file.close()?;

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

fn circuit_label(circ: &Circuit) -> anyhow::Result<String> {
    if circ.shortest_private_suffix != fixedascii_null::<44>()? {
        Ok(circ.shortest_private_suffix.to_string())
    } else {
        Ok(circ.domain.to_string())
    }
}

fn write_day_index(file: &hdf5::File, index: HashMap<u8, Vec<u32>>) -> anyhow::Result<()> {
    let pb = pb_new(index.len(), format!("Writing day index"));

    let group = file.create_group("/index/day")?;

    for (day, indices) in index.into_iter() {
        group
            .new_dataset_builder()
            .with_data(&Array1::from_vec(indices))
            .create(format!("{day}").as_str())?;
        pb.inc(1);
    }

    const DAY_NOTE: &str =
        "Provides a cached copy of the indices into the circuits dataset of those \
        circuits that were observed on a given day.";
    group
        .new_attr_builder()
        .with_data(&arr0(fixedascii_from_str::<128>(DAY_NOTE)?))
        .create("note")?;

    pb.finish();
    Ok(())
}

fn write_label_index(file: &hdf5::File, index: HashMap<String, Vec<u32>>) -> anyhow::Result<()> {
    let pb = pb_new(index.len(), format!("Writing label index"));

    let group = file.create_group("/index/label")?;

    for (label, indices) in index.into_iter() {
        // We need the `replace("/", "_")` to maintain the path structure in the hdf5.
        group
            .new_dataset_builder()
            .with_data(&Array1::from_vec(indices))
            .create(label.replace("/", "_").as_str())?;
        pb.inc(1);
    }

    const LABEL_NOTE: &str =
        "Provides a cached copy of the indices into the circuits dataset of those \
        circuits that match the given label. The label is the circuit's \
        shortest_private_suffix, or the domain if the shortest_private_suffix \
        is null. The label path is modified to replace '/' with '_'.";
    group
        .new_attr_builder()
        .with_data(&arr0(fixedascii_from_str::<512>(LABEL_NOTE)?))
        .create("note")?;

    pb.finish();
    Ok(())
}

fn write_uuid_index(file: &hdf5::File, index: HashMap<String, Vec<u32>>) -> anyhow::Result<()> {
    let pb = pb_new(index.len(), format!("Writing uuid index"));

    let group = file.create_group("/index/uuid")?;

    for (uuid, indices) in index.into_iter() {
        if indices.len() != 1 {
            bail!(
                "Uuid should be unique but we found {} indices",
                indices.len()
            );
        }
        group
            .new_dataset_builder()
            .with_data(&arr0(indices[0]))
            .create(uuid.as_str())?;
        pb.inc(1);
    }

    const UUID_NOTE: &str =
        "Provides a cached copy of the indices into the circuits dataset of the \
        circuit with the given uuid.";
    group
        .new_attr_builder()
        .with_data(&arr0(fixedascii_from_str::<128>(UUID_NOTE)?))
        .create("note")?;

    pb.finish();
    Ok(())
}
