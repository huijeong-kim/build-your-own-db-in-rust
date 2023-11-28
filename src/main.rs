use libc::EXIT_FAILURE;
use my_sqlite::repl;
use std::env;
use std::process::exit;

fn main() {
    let argv: Vec<String> = env::args().collect();
    if argv.len() < 2 {
        println!("Must supply a database filename.");
        exit(EXIT_FAILURE);
    }

    repl::start(argv[1].clone());
}
