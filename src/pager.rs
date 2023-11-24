use crate::data::ROW_SIZE;
use crate::table::{PAGE_SIZE, ROWS_PER_PAGE, TABLE_MAX_PAGES};
use libc::EXIT_FAILURE;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::process::exit;

pub struct Pager {
    file: File,
    pages: [Option<[u8; PAGE_SIZE]>; TABLE_MAX_PAGES],
}

impl Pager {
    pub fn open(filename: &String) -> Self {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
        {
            Ok(f) => f,
            Err(_) => {
                println!("Unable to open file");
                exit(EXIT_FAILURE);
            }
        };

        Pager {
            file,
            pages: [None; TABLE_MAX_PAGES],
        }
    }

    pub fn close(&mut self, num_rows: usize) {
        let num_full_pages = num_rows / ROWS_PER_PAGE;
        let num_additional_rows = num_rows % ROWS_PER_PAGE;

        for page_number in 0..num_full_pages {
            if self.pages[page_number].is_none() {
                continue;
            }
            self.flush_page(page_number, PAGE_SIZE);
            self.pages[page_number] = None;
        }

        if num_additional_rows > 0 {
            let page_num = num_full_pages;
            if self.pages[page_num].is_some() {
                self.flush_page(page_num, num_additional_rows * ROW_SIZE);
                self.pages[page_num] = None;
            }
        }
    }

    fn flush_page(&mut self, page_num: usize, size: usize) {
        if self.pages[page_num].is_none() {
            println!("Tried to flush null page");
            exit(EXIT_FAILURE);
        }

        match self
            .file
            .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
        {
            Ok(_) => {}
            Err(e) => {
                println!("Error seeking: {:?}", e.raw_os_error());
            }
        }

        let page = self.pages[page_num].as_ref().unwrap();
        match self.file.write(&page[..size]) {
            Ok(_) => {}
            Err(e) => {
                println!("Error writing: {:?}", e.raw_os_error());
                exit(EXIT_FAILURE);
            }
        }
    }

    pub fn file_size(&self) -> u64 {
        self.file.metadata().unwrap().len()
    }

    pub fn page(&mut self, page_num: usize) -> *mut u8 {
        if page_num > TABLE_MAX_PAGES {
            println!(
                "Tried to fetch page number out of bounds. {} > {}",
                page_num, TABLE_MAX_PAGES
            );
            exit(EXIT_FAILURE);
        }

        let mut page = [0; PAGE_SIZE];

        if self.pages[page_num].is_none() {
            // Cache miss. Allocate memory and load from file.
            let file_length = self.file.metadata().unwrap().len() as usize;
            let mut num_pages = file_length / PAGE_SIZE;
            if file_length % PAGE_SIZE != 0 {
                num_pages += 1;
            }

            if page_num <= num_pages as usize {
                self.file
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .unwrap();

                let len_to_read = if (page_num + 1) * PAGE_SIZE > file_length {
                    file_length - (page_num * PAGE_SIZE)
                } else {
                    PAGE_SIZE
                };

                if let Err(e) = self.file.read_exact(&mut page[0..len_to_read]) {
                    println!("Error reading file: {:?}", e.raw_os_error());
                    exit(EXIT_FAILURE);
                }
            }

            self.pages[page_num] = Some(page);
        }

        self.pages[page_num].as_ref().unwrap().as_ptr() as *mut _
    }
}
