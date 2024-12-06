use crate::processing_error;
use std::{collections::HashMap, fs::File, io::Write};

use processing_error::ProcessingError;
use serde_json::Value;

// Batched writer struct to handle buffered writes
pub struct BatchedWriter {
    csv_writers: HashMap<String, Vec<(String, String)>>,
    kv_entries: Vec<Value>,
    batch_size: usize,
    total_csv_writers: HashMap<String, csv::Writer<File>>,
    kv_file: File,
    output_format: String,
}

impl BatchedWriter {
    pub fn new(
        csv_writers: HashMap<String, csv::Writer<File>>,
        kv_file: File,
        output_format: String,
        batch_size: usize,
    ) -> Self {
        BatchedWriter {
            csv_writers: HashMap::new(),
            total_csv_writers: csv_writers,
            kv_entries: Vec::with_capacity(batch_size),
            kv_file,
            output_format,
            batch_size,
        }
    }

    pub fn add_csv_entry(
        &mut self,
        entity_type: String,
        record: (String, String),
    ) -> Result<(), ProcessingError> {
        self.csv_writers
            .entry(entity_type)
            .or_insert_with(Vec::new)
            .push(record);

        // Flush if batch is full
        if self.kv_entries.len() >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }

    pub fn add_kv_entry(&mut self, entry: Value) -> Result<(), ProcessingError> {
        self.kv_entries.push(entry);

        // Flush if batch is full
        if self.kv_entries.len() >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), ProcessingError> {
        // Flush CSV entries
        for (entity_type, entries) in &self.csv_writers {
            if let Some(writer) = self.total_csv_writers.get_mut(entity_type) {
                for (label, entity_id) in entries {
                    writer.write_record(&[label, entity_id])?;
                }
            }
        }
        self.csv_writers.clear();

        // Flush KV entries
        if !self.kv_entries.is_empty() {
            for entry in &self.kv_entries {
                match self.output_format.as_str() {
                    "JSONLines" => {
                        writeln!(self.kv_file, "{}", serde_json::to_string(entry)?)?;
                    }
                    _ => {
                        let kv_bytes = rmp_serde::encode::to_vec(entry)?;
                        self.kv_file.write_all(&kv_bytes)?;
                    }
                }
            }
            self.kv_entries.clear();
        }

        Ok(())
    }

    // Ensure any remaining entries are written on drop
    pub fn finalize(&mut self) -> Result<(), ProcessingError> {
        self.flush()?;

        // Close and flush all CSV writers
        for writer in self.total_csv_writers.values_mut() {
            writer.flush()?;
        }

        Ok(())
    }
}
