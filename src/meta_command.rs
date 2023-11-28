use crate::node_layout::print_constants;
use crate::table::Table;
use libc::EXIT_SUCCESS;
use std::process::exit;

pub enum MetaCommandResult {
    UnrecognizedCommand,
}
impl std::fmt::Debug for MetaCommandResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let display = match self {
            MetaCommandResult::UnrecognizedCommand => "Unrecognized command",
        };

        write!(f, "{}", display)
    }
}

pub fn do_meta_command(input: &str, table: &mut Table) -> Result<(), MetaCommandResult> {
    match input {
        ".exit" => {
            table.db_close();
            exit(EXIT_SUCCESS);
        }
        ".constants" => {
            println!("Constants:");
            print_constants();
        }
        ".btree" => {
            println!("Tree:");
            table.print();
        }
        _ => {
            return Err(MetaCommandResult::UnrecognizedCommand);
        }
    }

    Ok(())
}
