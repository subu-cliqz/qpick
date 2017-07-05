use std::fs::File;
use std::io::BufWriter;
use fst::{Error, raw, Streamer};

const PROGRESS: u64 = 1_000_000;

pub fn merge(dir_path: &str, nr_shards: usize) -> Result<(), Error>{
    let mut fsts = vec![];
    for i in 0..nr_shards {
        let fst = try!(raw::Fst::from_path(format!("{}/map.{}", dir_path, i)));
        fsts.push(fst);
    }
    let mut union = fsts.iter().collect::<raw::OpBuilder>().union();

    let wtr = BufWriter::new(try!(File::create(format!("{}/union_map.{}.fst", dir_path, nr_shards))));
    let mut builder = try!(raw::Builder::new(wtr));

    let mut count: u64 = 0;
    while let Some((k, vs)) = union.next() {
        builder.insert(k, vs[0].value).unwrap();
        count += 1;
        if count % PROGRESS == 0 {
            println!("Merging {:.1}M ...", count / PROGRESS);
        }

    }
    try!(builder.finish());

    Ok(())
}
