use std::io;

use docopt::Docopt;
use Error;

use qpick;

const USAGE: &'static str = "
Get vector ids and scores for ANN.
Usage:
    qpick merge [options] <path> <nr-shards>
    qpick merge --help
Options:
    -h, --help  Arg path is a path string. Arg nr-shards is a number of shards to merge.
";

#[derive(Debug, RustcDecodable)]
struct Args {
    arg_path: String,
    arg_nr_shards: usize,
}

pub fn run(argv: Vec<String>) -> Result<(), Error> {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.argv(&argv).decode())
        .unwrap_or_else(|e| e.exit());

    let r = qpick::Qpick::merge(args.arg_path, args.arg_nr_shards);
    println!("{:?}", r);

    Ok(())

}
