use std::collections::HashMap;
use std::path::PathBuf;

use clap::Parser;
use env_logger::{Builder, Target};
use hdf5::{
    types::{FixedAscii, VarLenArray},
    File, H5Type,
};
use indicatif::{ProgressBar, ProgressStyle};
use log::{self, LevelFilter};
use ndarray::{self, Array1, ArrayView};

use gtt23::{Circuit, CircuitIndex, IndexArrayEntry, IndexEntry};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// Writes an index into an HDF5 dataset of GTT23 circuits
pub struct Cli {
    /// Input paths to an hdf5 file containing a circuits dataset
    #[arg(value_name = "PATH", required = true)]
    pub input: PathBuf,
}

fn main() -> anyhow::Result<()> {
    Builder::new()
        .target(Target::Stderr)
        .filter_level(LevelFilter::Info)
        .init();

    let cli = Cli::parse();

    let mut ci_uuid = HashMap::<FixedAscii<32>, Vec<CircuitIndex>>::new();
    let mut ci_label = HashMap::<FixedAscii<44>, Vec<CircuitIndex>>::new();
    let mut ci_day = HashMap::<u8, Vec<CircuitIndex>>::new();
    let mut ci_port = HashMap::<u16, Vec<CircuitIndex>>::new();
    let mut ci_len = HashMap::<u16, Vec<CircuitIndex>>::new();

    // Read the entire dataset to compute the index.
    {
        let file = File::open(&cli.input)?;
        let dataset = file.dataset("/circuits")?;
        let size = dataset.size();
        //let step = dataset.chunk().map_or(1_000, |v| *v.first().unwrap_or(&1_000));
        let step = 1_000; // multiple of chunk size

        let pb = pb_new(size, format!("Computing index"));

        // Read from dataset in batches for better performance.
        for begin in (0..size).step_by(step) {
            let end = std::cmp::min(begin + step, size);

            let circuits: Array1<Circuit> = dataset.read_slice(ndarray::s![begin..end])?;

            for (i, circuit) in circuits.iter().enumerate() {
                let index = (begin + i) as CircuitIndex;

                ci_uuid.entry(circuit.uuid).or_default().push(index);
                ci_label.entry(circuit.label()).or_default().push(index);
                ci_day.entry(circuit.day).or_default().push(index);
                ci_port.entry(circuit.port).or_default().push(index);
                ci_len.entry(circuit.len).or_default().push(index);
            }

            pb.inc((end - begin) as u64);
        }

        pb.finish();
        file.close()?;
    }

    // Write each index into the hdf5 database.

    let mut index = create_index_entries(ci_uuid, "uuid")?;
    index.sort_by_key(|v| v.value.to_string());
    write_index(&cli.input, "/index/uuid", &Array1::from_vec(index))?;

    let mut index = create_index_arr_entries(ci_label, "label")?;
    index.sort_by_key(|v| v.value.to_string());
    write_index(&cli.input, "/index/label", &Array1::from_vec(index))?;

    let mut index = create_index_arr_entries(ci_day, "day")?;
    index.sort_by_key(|v| v.value);
    write_index(&cli.input, "/index/day", &Array1::from_vec(index))?;

    let mut index = create_index_arr_entries(ci_port, "port")?;
    index.sort_by_key(|v| v.value);
    write_index(&cli.input, "/index/port", &Array1::from_vec(index))?;

    let mut index = create_index_arr_entries(ci_len, "len")?;
    index.sort_by_key(|v| v.value);
    write_index(&cli.input, "/index/len", &Array1::from_vec(index))?;

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

pub fn create_index_entries<T>(
    index_map: HashMap<T, Vec<CircuitIndex>>,
    name: &str,
) -> anyhow::Result<Vec<IndexEntry<T>>>
where
    T: H5Type,
{
    Ok(create_index_arr_entries(index_map, name)?
        .into_iter()
        .map(|ent| IndexEntry {
            value: ent.value,
            index: *ent.indexarr.first().unwrap(),
        })
        .collect())
}

pub fn create_index_arr_entries<T>(
    index_map: HashMap<T, Vec<CircuitIndex>>,
    name: &str,
) -> anyhow::Result<Vec<IndexArrayEntry<T>>>
where
    T: H5Type,
{
    let mut index = Vec::new();
    let pb = pb_new(index_map.len(), format!("Preparing {name} index"));

    for (value, mut indices) in index_map.into_iter() {
        indices.sort();
        let indexarr = VarLenArray::from_slice(&indices);
        index.push(IndexArrayEntry { value, indexarr });
        pb.inc(1);
    }

    pb.finish();
    Ok(index)
}

pub fn write_index<'d, A, T, D>(path: &PathBuf, name: &str, data: A) -> anyhow::Result<()>
where
    A: Into<ArrayView<'d, T, D>>,
    T: H5Type,
    D: ndarray::Dimension,
{
    let file = File::open_rw(path)?;

    if let Ok(_) = file.dataset(name) {
        // Note this unlinks but does not reclaim its storage space.
        file.unlink(name)?;
    }

    file.new_dataset_builder().with_data(data).create(name)?;

    file.close()?;
    Ok(())
}
