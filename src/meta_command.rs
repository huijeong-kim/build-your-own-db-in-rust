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

pub fn do_meta_command(input: &str) -> Result<(), MetaCommandResult> {
    if input == ".exit" {
        exit(EXIT_SUCCESS);
    } else {
        return Err(MetaCommandResult::UnrecognizedCommand);
    }
}
