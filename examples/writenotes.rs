use std::path::PathBuf;

use clap::Parser;
use env_logger::{Builder, Target};
use hdf5::{types::VarLenAscii, File};
use log::{self, LevelFilter};
use ndarray;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// Create an HDF5 dataset from GTT23 circuits encoded in jsonl files
pub struct Cli {
    /// Input paths to an hdf5 file containing a circuits dataset
    #[arg(value_name = "PATH", required = true)]
    pub input: PathBuf,
}

const CIRCUITS_NOTE: &str = "Circuit data as measured from exit relays in \
    the live Tor network. Further description of the dataset can be found \
    in the research paper 'Website Fingerprinting with Genuine Tor Traces' \
    by Rob Jansen, Ryan Wails, and Aaron Johnson. Please cite if you use \
    this dataset.";

const UUID_NOTE: &str = "Provides a cached copy of the indices into the \
    circuits dataset of the circuit with the given uuid.";

const LABEL_NOTE: &str = "Provides a cached copy of the indices into the \
    circuits dataset of those circuits that match the given label. The label \
    is the circuit's shortest_private_suffix, or the domain if the \
    shortest_private_suffix is null.";

const DAY_NOTE: &str = "Provides a cached copy of the indices into the \
    circuits dataset of those circuits that were observed on a given day.";

const PORT_NOTE: &str = "Provides a cached copy of the indices into the \
    circuits dataset of those circuits with the given port.";

const LEN_NOTE: &str = "Provides a cached copy of the indices into the \
    circuits dataset of those circuits with the given length.";

fn main() -> anyhow::Result<()> {
    Builder::new()
        .target(Target::Stderr)
        .filter_level(LevelFilter::Info)
        .init();

    let cli = Cli::parse();

    let file = File::open_rw(&cli.input)?;

    write_dataset_note(&file, "/circuits", CIRCUITS_NOTE)?;
    write_dataset_note(&file, "/index/uuid", UUID_NOTE)?;
    write_dataset_note(&file, "/index/label", LABEL_NOTE)?;
    write_dataset_note(&file, "/index/day", DAY_NOTE)?;
    write_dataset_note(&file, "/index/port", PORT_NOTE)?;
    write_dataset_note(&file, "/index/len", LEN_NOTE)?;

    file.close()?;
    Ok(())
}

fn write_dataset_note(file: &File, name: &str, note: &str) -> anyhow::Result<()> {
    let dataset = file.dataset(name)?;
    let note_data = ndarray::arr0(VarLenAscii::from_ascii(note)?);

    if let Ok(attr) = dataset.attr("note") {
        attr.write(&note_data)?;
    } else {
        dataset
            .new_attr_builder()
            .with_data(&note_data)
            .create("note")?;
    }

    Ok(())
}

fn _write_group_note(file: &File, name: &str, note: &str) -> anyhow::Result<()> {
    let group = file.group(name)?;
    let note_data = ndarray::arr0(VarLenAscii::from_ascii(note)?);

    if let Ok(attr) = group.attr("note") {
        attr.write(&note_data)?;
    } else {
        group
            .new_attr_builder()
            .with_data(&note_data)
            .create("note")?;
    }

    Ok(())
}
