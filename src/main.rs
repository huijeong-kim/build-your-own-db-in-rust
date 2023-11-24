use std::ffi::CString;
use std::io::Write;
use std::ptr::null_mut;
use std::process::exit;

use libc::{EXIT_SUCCESS, EXIT_FAILURE};
use libc::c_char;

#[repr(C)]
struct InputBuffer {
    buffer: *mut c_char,
    buffer_length: usize,
    input_length: isize,
}
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
    let mut input_buffer = InputBuffer::new();
    loop {
        print_prompt();
        read_input(&mut input_buffer);

        let input_buffer_str = c_char_to_string(input_buffer.buffer);
        if input_buffer_str == ".exit" {
            unsafe { close_input_buffer(&mut input_buffer); }
            exit(EXIT_SUCCESS);
        } else {
            println!("Unrecognized command '{}'.\n", input_buffer_str);
        }
        
    }
}

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

fn read_input(input_buffer: &mut InputBuffer) {
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

unsafe fn copy_to_buffer(src: *const u8, len: libc::size_t) -> *mut i8 { 
    let buffer = libc::malloc(len) as *mut libc::c_char;
    if buffer.is_null() {
        eprintln!("Memory allocation failed");
        std::process::exit(EXIT_FAILURE);
    }

    std::ptr::copy_nonoverlapping(src as *const libc::c_void, buffer as *mut libc::c_void, len);

    buffer
}

unsafe fn close_input_buffer(input_buffer: &mut InputBuffer) {
    libc::free(input_buffer.buffer as *mut libc::c_void);
}
