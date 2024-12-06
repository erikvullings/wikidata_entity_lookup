use clap::{Arg, ArgAction, Command};
use std::fs::create_dir_all;
use std::path::Path;

use crate::processing_error::ProcessingError;

#[derive(Debug, Clone)]
pub struct Config {
    /// P31 instance of entity types to extract
    pub entity_types: Vec<String>,
    /// Preferred language for the results
    pub lang: String,
    /// Output format, either JSONL or MessagePack
    pub output_format: String,
    /// Output directory, will be created automatically if it doesn't exist
    pub output_dir: String,
    /// Extract images as base64 strings. If not, only the image filename is returned.
    pub process_images: bool,
}

/// Get the input file and additional configuration settings
pub fn get_configuration() -> Result<(String, Config), ProcessingError> {
    let matches = Command::new("Wikidata Entity Extraction")
      .version("1.0")
      .author("Erik Vullings")
      .about("Extracts and processes Wikidata for OSINT analysis")
      .arg(Arg::new("entity_types")
          .short('e')
          .long("entity-types")
          .help("Comma-separated list of entity types to process (e.g., person, organization, location)")
          .default_value("person,organization,scientific_organization,research_institute,government_agency,event,mood")
          .value_delimiter(',')
          .num_args(1..))
      .arg(Arg::new("lang")
          .short('l')
          .long("lang")
          .help("Language for labels and descriptions")
          .default_value("en"))
      .arg(Arg::new("output_format")
          .short('f')
          .long("format")
          .help("Output format for key-value store (MessagePack or JSONLines)")
          .default_value("MessagePack"))
      .arg(Arg::new("output_dir")
          .short('o')
          .long("output")
          .help("Output directory")
          .default_value("output"))
      .arg(Arg::new("input_file")
          .help("Path to the Wikidata JSON dump")
          .required(true)
          .index(1))
      .arg(Arg::new("process_images")
          .short('i')
          .long("process-images")
          .help("Process images")
          .action(ArgAction::SetTrue) // This makes it a flag, not requiring a value
          .default_value("false"))
      .get_matches();
    let entity_types: Vec<String> = matches
        .get_many::<String>("entity_types")
        .unwrap()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let lang = matches.get_one::<String>("lang").unwrap().to_string();
    let output_format = matches
        .get_one::<String>("output_format")
        .unwrap()
        .trim()
        .to_string();
    let output_dir = matches
        .get_one::<String>("output_dir")
        .unwrap()
        .trim()
        .to_string();
    let process_images = matches.get_flag("process_images");
    let output_path = Path::new(&output_dir);
    if !output_path.exists() {
        create_dir_all(output_path)?;
    }
    let input_file = matches.get_one::<String>("input_file").unwrap().to_string();
    let config = Config {
        entity_types,
        lang,
        output_format,
        output_dir,
        process_images,
    };
    Ok((input_file, config))
}
