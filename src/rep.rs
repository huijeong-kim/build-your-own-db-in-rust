use std::io::Write;
use std::process::exit;
use libc::EXIT_FAILURE;

use crate::{
    meta_command::do_meta_command,
    statement::{execute_statement, prepare_statement},
};

pub fn start() {
    loop {
        print_prompt();
        let input = read_input();
        if input.starts_with('.') {
            if let Err(e) = do_meta_command(&input) {
                println!("{:?} '{}'", e, input);
            }
        } else {
            match prepare_statement(&input) {
                Ok(statement) => {
                    execute_statement(statement);
                    println!("Executed.");
                }
                Err(e) => {
                    println!("{:?} {}", e, input);
                }
            }
        }
    }
}

fn print_prompt() {
    print!("db > ");
    std::io::stdout().flush().expect("Failed to flush stdout");
}

fn read_input() -> String {
    let mut input = String::new();

    if let Ok(bytes_read) = std::io::stdin().read_line(&mut input) {
        if bytes_read <= 0 {
            println!("Error reading input\n");
            exit(EXIT_FAILURE);
        }
    } else {
        eprintln!("Error reading input");
        exit(EXIT_FAILURE);
    }

    input.pop(); // remove '\n'

    input
}
