use libc::c_char;
use libc::{EXIT_FAILURE, EXIT_SUCCESS};
use std::ffi::CString;
use std::io::Write;
use std::process::exit;
use std::ptr::null_mut;

#[repr(C)]
struct InputBuffer {
    buffer: *mut c_char,
    buffer_length: usize,
    input_length: isize,
}
#[allow(dead_code)]
impl InputBuffer {
    pub fn new() -> InputBuffer {
        InputBuffer {
            buffer: null_mut(),
            buffer_length: 0,
            input_length: 0,
        }
    }
}

fn main() {
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

#[allow(dead_code)]
fn c_char_to_string(buffer: *mut c_char) -> String {
    unsafe {
        let input_buffer_str = CString::from_raw(buffer);
        if input_buffer_str.as_ptr() == std::ptr::null() {
            eprintln!("Memory allocation failed");
            std::process::exit(EXIT_FAILURE);
        }
        input_buffer_str.into_string().unwrap()
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
            exit(libc::EXIT_FAILURE);
        }
    } else {
        eprintln!("Error reading input");
        std::process::exit(EXIT_FAILURE);
    }

    input.pop(); // remove '\n'

    input
}

fn _read_input(input_buffer: &mut InputBuffer) {
    let mut input = String::new();

    if let Ok(bytes_read) = std::io::stdin().read_line(&mut input) {
        if bytes_read <= 0 {
            println!("Error reading input\n");
            exit(libc::EXIT_FAILURE);
        }

        let input_length = bytes_read - 1; // ignore newline
        unsafe {
            (*input_buffer).buffer = copy_to_buffer(input.as_ptr(), input_length);
        }
        (*input_buffer).buffer_length = input_length as usize;
    } else {
        eprintln!("Error reading input");
        std::process::exit(EXIT_FAILURE);
    }
}

#[allow(dead_code)]
unsafe fn copy_to_buffer(src: *const u8, len: libc::size_t) -> *mut i8 {
    let buffer = libc::malloc(len) as *mut libc::c_char;
    if buffer.is_null() {
        eprintln!("Memory allocation failed");
        std::process::exit(EXIT_FAILURE);
    }

    std::ptr::copy_nonoverlapping(src as *const libc::c_void, buffer as *mut libc::c_void, len);

    buffer
}

#[allow(dead_code)]
unsafe fn close_input_buffer(input_buffer: &mut InputBuffer) {
    libc::free(input_buffer.buffer as *mut libc::c_void);
}

enum MetaCommandResult {
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

fn do_meta_command(input: &str) -> Result<(), MetaCommandResult> {
    if input == ".exit" {
        exit(EXIT_SUCCESS);
    } else {
        return Err(MetaCommandResult::UnrecognizedCommand);
    }
}

enum StatementType {
    Insert,
    Select,
}

struct Statement {
    s_type: StatementType,
}

enum PrepareResult {
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

fn prepare_statement(buffer: &String) -> Result<Statement, PrepareResult> {
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

fn execute_statement(statement: Statement) {
    match statement.s_type {
        StatementType::Insert => {
            println!("This is where we would do an insert");
        }
        StatementType::Select => {
            println!("This is where we would do an select")
        }
    }
}
