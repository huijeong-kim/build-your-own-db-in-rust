use libc::EXIT_FAILURE;
use std::io::Write;
use std::process::exit;

use crate::statement::{ExecuteResult, PrepareResult};
use crate::table::Table;
use crate::{
    meta_command::do_meta_command,
    statement::{execute_statement, prepare_statement},
};

pub fn start(db_filename: String) {
    let mut table = Table::new();
    table.db_open(&db_filename);

    loop {
        print_prompt();
        let input = read_input();
        if input.starts_with('.') {
            if let Err(e) = do_meta_command(&input, &mut table) {
                println!("{:?} '{}'", e, input);
            }
        } else {
            match prepare_statement(&input) {
                Ok(statement) => match execute_statement(statement, &mut table) {
                    Ok(_) => {
                        println!("Executed.");
                    }
                    Err(ExecuteResult::TableFull) => {
                        println!("Error: Table full.");
                    }
                    Err(ExecuteResult::DuplicateKey) => {
                        println!("Error: Duplicate key.");
                    }
                },
                Err(PrepareResult::SyntaxError) => {
                    println!("Syntax error. Could not parse statement");
                }
                Err(PrepareResult::UnrecognizedCommand) => {
                    println!("Unrecognized keyword at start of '{}'", input);
                }
                Err(PrepareResult::StringTooLong) => {
                    println!("String is too long.");
                }
                Err(PrepareResult::NegativeId) => {
                    println!("ID must be positive.");
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
