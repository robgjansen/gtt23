use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::bail;
use clap::Parser;
use env_logger::{Builder, Target};
use hdf5::{
    types::{FixedAscii, VarLenArray},
    File, H5Type,
};
use indicatif::{ProgressBar, ProgressStyle};
use log::{self, LevelFilter};
use ndarray::{self, Array1, ArrayView};

use gtt23::{Circuit, DayIndexEntry, LabelIndexEntry, UuidIndexEntry};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// Create an HDF5 dataset from GTT23 circuits encoded in jsonl files
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

    let mut index_day = HashMap::<u8, Vec<u32>>::new();
    let mut index_uuid = HashMap::<FixedAscii<32>, Vec<u32>>::new();
    let mut index_label = HashMap::<FixedAscii<44>, Vec<u32>>::new();

    // Read the dataset to compute the index.
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
                let index = (begin + i) as u32;

                index_day.entry(circuit.day).or_default().push(index);
                index_uuid.entry(circuit.uuid).or_default().push(index);
                index_label.entry(circuit.label()).or_default().push(index);
            }

            pb.inc((end - begin) as u64);
        }

        pb.finish();
        file.close()?;
    }

    // Write the day index.
    {
        let pb = pb_new(index_day.len(), format!("Preparing day index"));
        let mut index = Vec::new();
        for (day, mut indices) in index_day.into_iter() {
            indices.sort();
            let indexa = VarLenArray::from_slice(&indices);
            index.push(DayIndexEntry { day, indexa });
            pb.inc(1);
        }
        index.sort_by_key(|v| v.day);
        pb.finish();

        write_index(&cli.input, "/index/day", &Array1::from_vec(index))?;
    }

    // Write the label index.
    {
        let pb = pb_new(index_label.len(), format!("Preparing label index"));
        let mut index = Vec::new();
        for (label, mut indices) in index_label.into_iter() {
            indices.sort();
            let indexa = VarLenArray::from_slice(&indices);
            index.push(LabelIndexEntry { label, indexa });
            pb.inc(1);
        }
        index.sort_by_key(|v| v.label.to_string());
        pb.finish();

        write_index(&cli.input, "/index/label", &Array1::from_vec(index))?;
    }

    // Write the uuid index.
    {
        let pb = pb_new(index_uuid.len(), format!("Preparing uuid index"));
        let mut index = Vec::new();
        for (uuid, indices) in index_uuid.into_iter() {
            if indices.len() != 1 {
                bail!("Too many indieces: {}", indices.len());
            }
            index.push(UuidIndexEntry {
                uuid,
                index: *indices.first().unwrap(),
            });
            pb.inc(1);
        }
        index.sort_by_key(|v| v.uuid.to_string());
        pb.finish();

        write_index(&cli.input, "/index/uuid", &Array1::from_vec(index))?;
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
