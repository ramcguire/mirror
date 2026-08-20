//! Exports both [`MirroredVolume`] and [`BackendNode`] CRDs as a multi-doc
//! YAML stream, generated from the structs in `mirrorvol-api`.
//!
//! Run via:
//!
//! ```sh
//! cargo run --bin crdgen > deploy/crds/mirroredvolume.yaml
//! ```
//!
//! and check the output into git.
//!
//! `--check <path>` compares the freshly generated YAML against that
//! already-committed file in memory and exits non-zero on a mismatch.

use kube::CustomResourceExt;
use mirrorvol_api::{BackendNode, MirroredVolume};
use std::{env, fs, process::ExitCode};

fn generate() -> String {
    let mirrored_volume = MirroredVolume::crd();
    let backend_node = BackendNode::crd();
    let mut out = serde_yaml::to_string(&mirrored_volume).expect("CRD schema always serializes");
    out.push_str("---\n");
    out.push_str(&serde_yaml::to_string(&backend_node).expect("CRD schema always serializes"));
    out
}

fn main() -> ExitCode {
    match env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [] => {
            print!("{}", generate());
            ExitCode::SUCCESS
        }
        [flag, path] if flag == "--check" => {
            let committed = match fs::read_to_string(path) {
                Ok(contents) => contents,
                Err(err) => {
                    eprintln!("error: reading {path}: {err}");
                    return ExitCode::FAILURE;
                }
            };
            if committed == generate() {
                ExitCode::SUCCESS
            } else {
                eprintln!("error: {path} is out of date relative to mirrorvol-api's CRD types.");
                eprintln!(
                    "Run `cargo run --bin crdgen > {path}` (or `task crds:generate`) and commit the result."
                );
                ExitCode::FAILURE
            }
        }
        _ => {
            eprintln!("usage: crdgen [--check <path>]");
            ExitCode::FAILURE
        }
    }
}
