use std::fmt::Formatter;

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

pub struct Row {
    pub id: i32,
    pub username: [u8; COLUMN_USERNAME_SIZE],
    pub email: [u8; COLUMN_EMAIL_SIZE],
}
impl Row {
    pub fn new() -> Self {
        Row {
            id: 0,
            username: [0; COLUMN_USERNAME_SIZE],
            email: [0; COLUMN_EMAIL_SIZE],
        }
    }
}

impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({}, {}, {})",
            self.id,
            String::from_utf8_lossy(&self.username).to_string().trim_matches('\u{0000}'),
            String::from_utf8_lossy(&self.email).to_string().trim_matches('\u{0000}')
        )
    }
}

const ID_SIZE: usize = std::mem::size_of::<i32>();
const USERNAME_SIZE: usize = COLUMN_USERNAME_SIZE;
const EMAIL_SIZE: usize = COLUMN_EMAIL_SIZE;

const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
pub const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

pub unsafe fn serialize_row(source: &Row, dest: *mut u8) {
    std::ptr::write(dest.add(ID_OFFSET) as *mut i32, source.id);
    std::ptr::write(
        dest.add(USERNAME_OFFSET) as *mut [u8; COLUMN_USERNAME_SIZE],
        source.username,
    );
    std::ptr::write(
        dest.add(EMAIL_OFFSET) as *mut [u8; COLUMN_EMAIL_SIZE],
        source.email,
    );
}

pub unsafe fn deserialize_row(source: *const u8, dest: &mut Row) {
    dest.id = std::ptr::read(source.add(ID_OFFSET) as *mut i32);
    dest.username = std::ptr::read(source.add(USERNAME_OFFSET) as *mut [u8; COLUMN_USERNAME_SIZE]);
    dest.email = std::ptr::read(source.add(EMAIL_OFFSET) as *mut [u8; COLUMN_EMAIL_SIZE]);
}
