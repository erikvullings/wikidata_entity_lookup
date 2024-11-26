use base64::engine::general_purpose;
use base64::Engine;
use clap::{Arg, ArgAction, Command};
use md5::{Digest, Md5};
use rand::Rng;
use rayon::prelude::*;
use reqwest::blocking::Client;
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, REFERER, USER_AGENT,
};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Mutex;

#[derive(Debug, Clone)]
struct Config {
    entity_types: Vec<String>,
    lang: String,
    output_format: String,
    output_dir: String,
    process_images: bool,
}

#[derive(Debug, Deserialize)]
struct WikidataEntity {
    claims: Option<Map<String, Value>>,
    id: String,
    labels: Option<Map<String, Value>>,
    descriptions: Option<Map<String, Value>>,
}

fn get_entity_type_mappings() -> HashMap<&'static str, &'static str> {
    HashMap::from([
        // human: https://www.wikidata.org/wiki/Q5
        ("person", "Q5"),
        ("organization", "Q43229"),
        ("location", "Q17334923"),
        ("event", "Q1656682"),
        ("creative_work", "Q17537576"),
    ])
}

fn get_default_properties() -> HashMap<&'static str, Vec<&'static str>> {
    HashMap::from([
        (
            // Person-related properties
            "person",
            vec![
                "P569",  // Date of birth, https://www.wikidata.org/wiki/Property:P569
                "P570",  // Date of death, https://www.wikidata.org/wiki/Property:P570
                "P27",   // Country of citizenship
                "P106",  // Occupation
                "P18",   // Image
                "P39",   // Position held
                "P1449", // Nickname
            ],
        ),
        (
            // Organization-related properties
            "organization",
            vec![
                "P17",   // Country
                "P112",  // Founder
                "P571",  // Inception date
                "P18",   // Image
                "P154",  // Logo
                "P1454", // Legal form
            ],
        ),
        (
            // Location-related properties
            "location",
            vec![
                "P625", // Coordinates
                "P17",  // Country
                "P18",  // Image
                "P421", // Time zone
            ],
        ),
        (
            // Event-related properties
            "event",
            vec![
                "P585", // Point in time
                "P17",  // Country
                "P276", // Location
                "P31",  // Instance of
                "P18",  // Image
            ],
        ),
        (
            // Creative work-related properties
            "creative_work",
            vec![
                "P50",  // Author
                "P577", // Publication date
                "P136", // Genre
                "P921", // Main subject
                "P18",  // Image
            ],
        ),
    ])
}

fn create_image_thumbnail_url(filename: &str, width: Option<u32>) -> Option<String> {
    // Step 1: Replace spaces with underscores
    let modified_filename = filename.replace(' ', "_");

    // Step 2: Compute the MD5 hash of the modified filename
    let mut hasher = Md5::new();
    hasher.update(modified_filename.as_bytes());
    let result = hasher.finalize();

    // Convert the hash to a hexadecimal string
    let hash_str = format!("{:x}", result);

    // Step 3: Extract the first two characters from the MD5 hash as `a` and `b`
    if let Some(ab) = hash_str.get(..2) {
        let a = &ab[0..1];

        // Step 4: Construct the base URL
        let base_url = format!(
            "https://upload.wikimedia.org/wikipedia/commons/thumb/{}/{}/{}",
            a, ab, modified_filename
        );

        // Step 5: Use provided width or default to 64px
        let thumbnail_url = format!(
            "{}/{}px-{}",
            base_url,
            width.unwrap_or(64),
            modified_filename
        );

        Some(thumbnail_url)
    } else {
        None
    }
}

fn generate_browser_headers() -> HeaderMap {
    let mut rng = rand::thread_rng();

    // List of plausible user agents
    let user_agents = vec![
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.3112.101 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.3; rv:122.0) Gecko/20100101 Firefox/122.0",
    ];

    // List of accept language headers
    let accept_languages = vec![
        "en-US,en;q=0.9",
        "en-GB,en;q=0.9",
        "en-CA,en;q=0.9",
        "en-AU,en;q=0.9",
    ];

    // List of potential referrers
    let referrers = vec![
        "https://www.google.com/",
        "https://www.bing.com/",
        "https://www.wikipedia.org/",
        "https://www.wikimedia.org/",
    ];

    let mut headers = HeaderMap::new();

    // Select random user agent
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static(user_agents[rng.gen_range(0..user_agents.len())]),
    );

    // Add Accept header
    headers.insert(
        ACCEPT,
        HeaderValue::from_static(
            "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        ),
    );

    // Select random accept language
    headers.insert(
        ACCEPT_LANGUAGE,
        HeaderValue::from_static(accept_languages[rng.gen_range(0..accept_languages.len())]),
    );

    // Add Accept-Encoding
    headers.insert(
        ACCEPT_ENCODING,
        HeaderValue::from_static("gzip, deflate, br"),
    );

    // Optionally add a referrer
    if rng.gen_bool(0.7) {
        // 70% chance of adding a referrer
        headers.insert(
            REFERER,
            HeaderValue::from_static(referrers[rng.gen_range(0..referrers.len())]),
        );
    }

    headers
}

// Updated fetch function
fn fetch_base64_image(commons_url: String) -> Result<String, reqwest::Error> {
    let client = Client::builder()
        .default_headers(generate_browser_headers())
        .build()?;

    let response = client.get(&commons_url).send()?;

    // Check the Content-Type header to ensure it's an image
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");

    if !content_type.starts_with("image/") {
        // eprintln!("Thumbnail could not be retrieved: {}", commons_url);
        return Ok(commons_url);
    }

    let image_bytes = response.bytes()?;
    Ok(general_purpose::STANDARD.encode(&image_bytes))
}

fn process_wikidata(
    input_path: &str,
    config: Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let entity_mappings = get_entity_type_mappings();
    let default_properties = get_default_properties();

    // Initialize CSV writers with mutex for thread safety
    let mut csv_writers: HashMap<String, Mutex<csv::Writer<File>>> = HashMap::new();
    for entity_type in &config.entity_types {
        let csv_path = format!("{}/{}.csv", config.output_dir, entity_type);
        csv_writers.insert(
            entity_type.clone(),
            Mutex::new(csv::Writer::from_path(csv_path)?),
        );
    }

    // Create KV store file with mutex
    let kv_file = Mutex::new(File::create(format!(
        "{}/entity_kv_store.{}",
        config.output_dir,
        match config.output_format.as_str() {
            "JSONLines" => "jsonl",
            _ => "msgpack",
        }
    ))?);

    // Process file in parallel using rayon
    let file = File::open(input_path)?;
    let reader = BufReader::new(file);

    reader.lines().par_bridge().try_for_each(
        |line_result| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            // Wrap the entire processing in a Result
            let line = match line_result {
                Ok(line) => line,
                Err(_) => return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(()), // Skip lines that can't be read
            };

            // Skip empty lines and array markers
            if line.trim().is_empty() || line.starts_with('[') || line.starts_with(']') {
                return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(());
            }

            // Remove trailing comma if present
            let json_str = line.trim_end_matches(',');

            // Parse entity
            let entity: WikidataEntity = match serde_json::from_str(json_str) {
                Ok(e) => e,
                Err(_) => return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(()), // Skip invalid JSON
            };

            // Process entity
            if let (Some(claims), Some(labels), Some(descriptions)) =
                (entity.claims, entity.labels, entity.descriptions)
            {
                let description = descriptions
                    .get(&config.lang)
                    .and_then(|obj| obj.get("value"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if let Some(label_obj) = labels.get(&config.lang) {
                    if let Some(label) = label_obj.get("value").and_then(|v| v.as_str()) {
                        for entity_type in &config.entity_types {
                            if let Some(instance_of) = entity_mappings.get(entity_type.as_str()) {
                                // https://www.wikidata.org/wiki/Property:P31 instance of
                                if claims.get("P31").and_then(|p31| p31.as_array()).map_or(
                                    false,
                                    |instances| {
                                        instances.iter().any(|i| {
                                            i["mainsnak"]["datavalue"]["value"]["id"]
                                                == *instance_of
                                        })
                                    },
                                ) {
                                    // Wrap file writing operations in a Result
                                    if let Err(_) = write_entity_data(
                                        &csv_writers,
                                        &kv_file,
                                        entity_type,
                                        &entity.id,
                                        label,
                                        description,
                                        &claims,
                                        &config,
                                        &default_properties,
                                    ) {
                                        // Log or handle write errors
                                        return Ok::<(), Box<dyn std::error::Error + Send + Sync>>(
                                            (),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        },
    )?;

    Ok(())
}

// Extract file writing logic to a separate function
fn write_entity_data(
    csv_writers: &HashMap<String, Mutex<csv::Writer<File>>>,
    kv_file: &Mutex<File>,
    entity_type: &str,
    entity_id: &str,
    label: &str,
    description: &str,
    claims: &Map<String, Value>,
    config: &Config,
    default_properties: &HashMap<&str, Vec<&str>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Write to CSV
    if let Some(writer) = csv_writers.get(entity_type) {
        let mut writer = writer.lock().unwrap();
        writer.write_record(&[label, entity_id])?;
    }

    // Extract and write properties
    let properties = extract_properties(
        entity_type,
        &Value::Object(claims.clone()),
        config.process_images,
        default_properties,
    );

    // Write to KV store
    let mut kv_file = kv_file.lock().unwrap();
    write_to_kv_store(
        &mut kv_file,
        entity_id,
        label,
        description,
        properties,
        &config.output_format,
    )?;

    Ok(())
}

fn extract_properties(
    entity_type: &str,
    claims: &Value,
    process_images: bool,
    default_properties: &HashMap<&str, Vec<&str>>,
) -> Value {
    let mut properties = serde_json::Map::new();

    match default_properties.get(entity_type) {
        Some(all_properties) => {
            for prop in all_properties {
                if let Some(value) = claims
                    .get(prop)
                    .and_then(|p| p.as_array())
                    .and_then(|array| array.get(0))
                {
                    match *prop {
                        "P569" | "P570" => {
                            // Simplify date fields (e.g., P569 = Date of Birth, P570 = Date of Death)
                            if let Some(date) = value
                                .get("mainsnak")
                                .and_then(|ms| ms.get("datavalue"))
                                .and_then(|dv| dv.get("value"))
                                .and_then(|v| v.get("time"))
                            {
                                // Strip precision and metadata, and format date
                                let simple_date =
                                    date.as_str().unwrap_or("").trim_start_matches('+');
                                properties.insert(
                                    prop.to_string(),
                                    Value::String(simple_date.to_string()),
                                );
                            }
                        }
                        "P27" | "P106" | "P39" | "P1449" => {
                            // Handle string or entity-id properties (e.g., country, occupation, position)
                            if let Some(id_value) = value
                                .get("mainsnak")
                                .and_then(|ms| ms.get("datavalue"))
                                .and_then(|dv| dv.get("value"))
                            {
                                properties.insert(prop.to_string(), id_value.clone());
                            }
                        }
                        "P18" => {
                            // Extract base64-decoded image (P18 = Image property)
                            if let Some(commons_url) = value
                                .get("mainsnak")
                                .and_then(|ms| ms.get("datavalue"))
                                .and_then(|dv| dv.get("value"))
                                .and_then(|v| v.as_str())
                                .and_then(|v| create_image_thumbnail_url(v, None))
                            {
                                if process_images {
                                    if let Ok(base64_image) = fetch_base64_image(commons_url) {
                                        properties.insert(
                                            "image".to_string(),
                                            Value::String(base64_image),
                                        );
                                    }
                                } else {
                                    properties
                                        .insert("image".to_string(), Value::String(commons_url));
                                }
                            }
                        }
                        _ => {
                            properties.insert(prop.to_string(), value.clone());
                        }
                    }
                }
                // if let Some(value) = claims
                //     .get(prop)
                //     .and_then(|p| p.as_array())
                //     .and_then(|array| array.get(0))
                // {
                //     properties.insert(prop.to_string(), value.clone());
                // }
            }
        }
        None => {}
    }
    Value::Object(properties)
}

fn write_to_kv_store(
    kv_file: &mut File,
    entity_id: &str,
    label: &str,
    description: &str,
    properties: Value,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let kv_entry = serde_json::json!({
        entity_id: {
            "label": label,
            "description": description,
            "properties": properties
        }
    });

    match format {
        "JSONLines" => {
            writeln!(kv_file, "{}", serde_json::to_string(&kv_entry)?)?;
        }
        _ => {
            let kv_bytes = rmp_serde::encode::to_vec(&kv_entry)?;
            kv_file.write_all(&kv_bytes)?;
        }
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Parse command-line arguments
    let matches = Command::new("Wikidata Entity Extraction")
        .version("1.0")
        .author("Erik Vullings")
        .about("Extracts and processes Wikidata for OSINT analysis")
        .arg(Arg::new("entity_types")
            .short('e')
            .long("entity-types")
            .help("Comma-separated list of entity types to process (e.g., person, organization, location)")
            .default_value("person,organization")
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

    // Extract arguments
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
    // Ensure the output directory exists
    let output_path = Path::new(&output_dir);
    if !output_path.exists() {
        create_dir_all(output_path)?;
    }
    let input_file = matches.get_one::<String>("input_file").unwrap();

    let config = Config {
        entity_types,
        lang,
        output_format,
        output_dir,
        process_images,
    };

    process_wikidata(input_file, config)
}
