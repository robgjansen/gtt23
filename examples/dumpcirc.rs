use std::path::PathBuf;

use clap::{Args, Parser};
use hdf5::{File, Result};
use ndarray::{s, Array0};

use gtt23::Circuit;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// Dump a circuit record from an HDF5 dataset of GTT23 circuits
pub struct Cli {
    /// Path to the HDF5 database file
    #[arg(value_name = "PATH")]
    pub path: PathBuf,
    /// HDF5 dataset name containing GTT23 circuits
    #[arg(short, long, value_name = "NAME", default_value = "circuits")]
    pub name: String,
    #[command(flatten)]
    pub select: Selector,
}

#[derive(Args)]
#[group(required = true, multiple = false)]
pub struct Selector {
    /// Select circuit by uuid
    #[arg(short, long)]
    pub uuid: Option<String>,
    /// Select circuit by index
    #[arg(short, long)]
    pub index: Option<usize>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Open the file for reading
    let file = File::open(cli.path)?;

    // Open the circuit dataset
    let ds = file.dataset(cli.name.as_str())?;

    // Get the index of the circuit
    let index = if let Some(i) = cli.select.index {
        i
    } else if let Some(_) = cli.select.uuid {
        unimplemented!()
    } else {
        panic!("No selector given")
    };

    // Grab a single circuit by its index in the circuit array
    let arr: Array0<Circuit> = ds.read_slice(s![index])?;

    match arr.first() {
        Some(circ) => println!("{:?}", circ),
        None => println!("Circuit not found at index {index}"),
    }

    // Note: we could dump multiple circuits like:
    // let arr: Array1<Circuit> = ds.read_slice(s![3..6])?;
    // for circ in arr.iter() {
    //     println!("{:?}", circ);
    // }

    Ok(())
}
