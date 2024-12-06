// Implement a custom error type that is Send + Sync
#[derive(Debug)]
pub enum ProcessingError {
    IoError(std::io::Error),
    JsonError(serde_json::Error),
    CsvError(csv::Error),
    MessagePackError(rmp_serde::encode::Error),
    // Other(String),
}

impl std::fmt::Display for ProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessingError::IoError(e) => write!(f, "IO Error: {}", e),
            ProcessingError::JsonError(e) => write!(f, "JSON Error: {}", e),
            ProcessingError::CsvError(e) => write!(f, "CSV Error: {}", e),
            ProcessingError::MessagePackError(e) => write!(f, "MessagePack Error: {}", e),
            // ProcessingError::Other(e) => write!(f, "Processing Error: {}", e),
        }
    }
}

impl std::error::Error for ProcessingError {}

impl From<std::io::Error> for ProcessingError {
    fn from(error: std::io::Error) -> Self {
        ProcessingError::IoError(error)
    }
}

impl From<serde_json::Error> for ProcessingError {
    fn from(error: serde_json::Error) -> Self {
        ProcessingError::JsonError(error)
    }
}

impl From<csv::Error> for ProcessingError {
    fn from(error: csv::Error) -> Self {
        ProcessingError::CsvError(error)
    }
}

impl From<rmp_serde::encode::Error> for ProcessingError {
    fn from(error: rmp_serde::encode::Error) -> Self {
        ProcessingError::MessagePackError(error)
    }
}
