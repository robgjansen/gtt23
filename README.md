# gtt23:

> [!WARNING]
> The state of this library is `experimental` and offered without any promise of
> support. Use at your own risk.

## What is this?

This is a helper library for interacting with the GTT23 dataset in Rust.

The GTT23 dataset contains network metadata of encrypted traffic measured from
exit relays in the Tor network over a 13-week measurement period in 2023. The
metadata is suitable for analyzing and evaluating website fingerprinting attacks
and defenses.

The dataset is available here:
https://doi.org/10.5281/zenodo.10620519
 
The measurement process, additional safety and ethical considerations, and a
statistical analysis of the dataset is presented in further detail in the
article "A Measurement of Genuine Tor Traces for Realistic Website
Fingerprinting", arXiv:2404.07892 \[cs.CR\].

The article is available here:
https://doi.org/10.48550/arXiv.2404.07892

If you find the dataset, the article, or this library useful in your own
research, please cite our work:

```
@techreport{gtt23-arxiv2024,
  title = {A Measurement of Genuine Tor Traces for Realistic Website Fingerprinting},
  author = {Jansen, Rob and Wails, Ryan and Johnson, Aaron},
  booktitle = {arXiv:2404.07892 [cs.CR]},
  year = {2024},
  doi = {10.48550/arXiv.2404.07892},
}
```

## How to use the library

The library helps you interact with the `gtt23.hdf5` dataset in Rust. You can add
this library as a dependency in the `Cargo.toml` file of your Rust project:

    cargo add --git https://github.com/robgjansen/gtt23.git

That will give you access to the definitions in `src/lib.rs`.

See the files in `examples/*.rs` for examples of how to use this library along with
the `hdf5` Rust bindings to interact with the `gtt23.hdf5` dataset in your project.
