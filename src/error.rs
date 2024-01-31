#[derive(Debug)]
pub enum DataFileError {
    //FIXME: remove Err prefix
    ErrNotADirectory,
    ErrIncompleteRead,
    ErrIncompleteWrite,
}

#[derive(Debug)]
pub enum KvError {
    KeyNotFound,
    EmptyValue,
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvError::KeyNotFound => write!(f, "Key not found"),
            KvError::EmptyValue => write!(f, "Value cannot be empty"),
        }
    }
    
}

impl std::fmt::Display for DataFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ErrNotADirectory => write!(f, "Invalid path. Require a path to directory"),
            Self::ErrIncompleteRead => write!(f, "Incomplete read"),
            Self::ErrIncompleteWrite => write!(f, "Incomplete write"),
        }
    }
}