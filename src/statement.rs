enum StatementType {
    Insert,
    Select,
}

pub struct Statement {
    s_type: StatementType,
}

pub enum PrepareResult {
    UnrecognizedCommand,
}
impl std::fmt::Debug for PrepareResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let display = match self {
            Self::UnrecognizedCommand => "Unrecognized keyword at start of",
        };

        write!(f, "{}", display)
    }
}

pub fn prepare_statement(buffer: &String) -> Result<Statement, PrepareResult> {
    let cmd = buffer.trim_start_matches('.');
    if cmd.starts_with("insert") {
        Ok(Statement {
            s_type: StatementType::Insert,
        })
    } else if cmd.starts_with("select") {
        Ok(Statement {
            s_type: StatementType::Select,
        })
    } else {
        Err(PrepareResult::UnrecognizedCommand)
    }
}

pub fn execute_statement(statement: Statement) {
    match statement.s_type {
        StatementType::Insert => {
            println!("This is where we would do an insert");
        }
        StatementType::Select => {
            println!("This is where we would do an select")
        }
    }
}
