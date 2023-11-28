use crate::row::{Row, COLUMN_EMAIL_SIZE, COLUMN_USERNAME_SIZE};
use crate::table::Table;

pub enum Statement {
    Insert(Row),
    Select,
}
impl Statement {
    pub fn new(args: &Vec<&str>) -> Result<Self, PrepareResult> {
        let command = args[0];
        match command {
            "insert" => Statement::new_insert(args),
            "select" => Statement::new_select(args),
            _ => Err(PrepareResult::UnrecognizedCommand)
        }
    }
    fn new_insert(args: &Vec<&str>) -> Result<Self, PrepareResult> {
        if args.len() < 4 {
            return Err(PrepareResult::SyntaxError);
        }

        let mut row = Row::new();
        let id = args[1].parse::<i32>().unwrap();
        if id < 0 {
            return Err(PrepareResult::NegativeId);
        }
        row.id = id as u32;

        let username_len = args[2].as_bytes().len();
        if username_len > COLUMN_USERNAME_SIZE {
            return Err(PrepareResult::StringTooLong);
        }
        row.username[..username_len].copy_from_slice(args[2].as_bytes());

        let email_len = args[3].as_bytes().len();
        if email_len > COLUMN_EMAIL_SIZE {
            return Err(PrepareResult::StringTooLong);
        }
        row.email[..email_len].copy_from_slice(args[3].as_bytes());

        Ok(Statement::Insert(row))
    }

    fn new_select(args: &Vec<&str>) -> Result<Self, PrepareResult> {
        if args.len() < 1 {
            return Err(PrepareResult::SyntaxError);
        }

        Ok(Statement::Select)
    }
}

pub enum PrepareResult {
    Success(Statement),
    UnrecognizedCommand,
    SyntaxError,
    StringTooLong,
    NegativeId,
}
impl PrepareResult {
    pub fn err_msg(&self, input: &String) -> String {
        match self {
            PrepareResult::Success(_) => { panic!("invalid call") }
            PrepareResult::UnrecognizedCommand => format!("Unrecognized keyword at start of '{}'", input),
            PrepareResult::SyntaxError => String::from("Syntax error. Could not parse statement"),
            PrepareResult::StringTooLong => String::from("String is too long."),
            PrepareResult::NegativeId => String::from("ID must be positive."),
        }
    }
}

pub fn prepare_statement(buffer: &String) -> PrepareResult {
    let args: Vec<&str> = buffer.split(' ').collect();
    match Statement::new(&args) {
        Ok(statement) => PrepareResult::Success(statement),
        Err(err) => err,
    }
}

pub enum ExecuteResult {
    Success,
    TableFull,
    DuplicateKey,
}
impl ExecuteResult {
    pub fn msg(&self) -> &str {
        match self {
            ExecuteResult::Success => { "Executed." }
            ExecuteResult::TableFull => { "Error: Table full." }
            ExecuteResult::DuplicateKey => { "Error: Duplicated key."}
        }
    }
}

pub fn execute_statement(statement: Statement, table: &mut Table) -> ExecuteResult {
    match statement {
        Statement::Insert(row) => table.insert(row),
        Statement::Select => table.select(),
    }
}
