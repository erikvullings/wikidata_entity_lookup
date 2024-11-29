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
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod entity_resolver;
use entity_resolver::EntityResolver;

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
        ("scientific_organization", "Q16519632"),
        ("research_institute", "Q31855"),
        ("government_agency", "Q327333"),
        ("association ", "Q15911314"),
        ("location", "Q17334923"),
        ("event", "Q1656682"),
        ("creative_work", "Q17537576"),
    ])
}

fn get_default_properties() -> HashMap<&'static str, Vec<&'static str>> {
    let organization_props = vec![
        "P17",   // Country
        "P112",  // Founder
        "P571",  // Inception date
        "P1813", // Short name
        "P18",   // Image
        "P154",  // Logo
        "P159",  // Headquarters locations
        "P856",  // Website
        "P749",  // Parent organisation
        "P1454", // Legal form
        "P3220", // KvK company ID
        "P452",  // industry
        "P101",  // field of work
    ];
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
                "P101",  // field of work
            ],
        ),
        ("organization", organization_props.clone()),
        ("scientific_organization", organization_props.clone()),
        ("research_institute", organization_props.clone()),
        ("government_agency", organization_props.clone()),
        ("association", organization_props.clone()),
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

// Implement a custom error type that is Send + Sync
#[derive(Debug)]
enum ProcessingError {
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

// Batched writer struct to handle buffered writes
struct BatchedWriter {
    csv_writers: HashMap<String, Vec<(String, String)>>,
    kv_entries: Vec<Value>,
    batch_size: usize,
    total_csv_writers: HashMap<String, csv::Writer<File>>,
    kv_file: File,
    output_format: String,
}

impl BatchedWriter {
    fn new(
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

    fn add_csv_entry(
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

    fn add_kv_entry(&mut self, entry: Value) -> Result<(), ProcessingError> {
        self.kv_entries.push(entry);

        // Flush if batch is full
        if self.kv_entries.len() >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), ProcessingError> {
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
    fn finalize(&mut self) -> Result<(), ProcessingError> {
        self.flush()?;

        // Close and flush all CSV writers
        for writer in self.total_csv_writers.values_mut() {
            writer.flush()?;
        }

        Ok(())
    }
}

fn process_wikidata(input_path: &str, config: Config) -> Result<(), ProcessingError> {
    let entity_mappings = get_entity_type_mappings();
    let default_properties = get_default_properties();

    // Create resolver with a specific cache file path
    let resolver = EntityResolver::new(
        PathBuf::from(format!("{}/entity_cache.csv", config.output_dir)),
        "https://www.wikidata.org/w/api.php".to_string(),
    );

    // Initialize CSV writers
    let mut csv_writers: HashMap<String, csv::Writer<File>> = HashMap::new();
    for entity_type in &config.entity_types {
        let csv_path = format!("{}/{}.csv", config.output_dir, entity_type);
        csv_writers.insert(entity_type.clone(), csv::Writer::from_path(csv_path)?);
    }

    // Create KV store file
    let kv_file = File::create(format!(
        "{}/entity_kv_store.{}",
        config.output_dir,
        match config.output_format.as_str() {
            "JSONLines" => "jsonl",
            _ => "msgpack",
        }
    ))?;

    // Create a batched writer
    let batch_size = 5000; // Adjust batch size as needed
    let batched_writer = BatchedWriter::new(
        csv_writers,
        kv_file,
        config.output_format.clone(),
        batch_size,
    );
    let batched_writer = Arc::new(Mutex::new(batched_writer));

    // Open input file and get total file size for progress tracking
    let file = File::open(input_path)?;
    let file_size = file.metadata()?.len();
    let reader = BufReader::new(file);

    // Progress tracking
    let start_time = Instant::now();
    let total_processed = AtomicU64::new(0);
    let last_reported_promille = AtomicU64::new(0);

    // Process file in parallel
    reader
        .lines()
        .par_bridge()
        .try_for_each(|line_result| -> Result<(), ProcessingError> {
            // Read line with thread-safe progress tracking
            let line = match line_result {
                Ok(line) => line,
                Err(e) => return Err(ProcessingError::IoError(e)),
            };

            // Skip empty or array marker lines
            if line.trim().is_empty() || line.starts_with('[') || line.starts_with(']') {
                return Ok(());
            }

            // Update progress using atomic operations
            let line_len = line.len() as u64;
            let current_total = total_processed.fetch_add(line_len, Ordering::Relaxed) + line_len; // it returns the previous value, so add line_len
            let current_promille = ((current_total as f64 / file_size as f64) * 1000.0) as u64;

            // Report progress with 0.1% granularity
            let last_promille = last_reported_promille.load(Ordering::Relaxed);
            if (current_promille - last_promille) >= 1 {
                // Use compare_exchange to ensure only one thread updates the progress
                if last_reported_promille
                    .compare_exchange(
                        last_promille,
                        current_promille,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    let elapsed = start_time.elapsed();
                    let eta = if current_promille > 0 {
                        let total_estimated_time =
                            elapsed.as_secs_f64() / (current_promille as f64 / 1000.0);
                        Duration::from_secs_f64(total_estimated_time - elapsed.as_secs_f64())
                    } else {
                        Duration::from_secs(0)
                    };

                    print!(
                        "\rProcessing: {:.1}% | Elapsed: {:.0}s | ETA: {:.0}s         ",
                        current_promille as f64 / 10.0,
                        elapsed.as_secs(),
                        eta.as_secs()
                    );
                    std::io::stdout().flush()?;
                }
            }

            // Remove trailing comma if present
            let json_str = line.trim_end_matches(',');

            // Parse entity
            let entity: WikidataEntity = match serde_json::from_str(json_str) {
                Ok(e) => e,
                Err(_) => return Ok(()),
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
                                if claims.get("P31").and_then(|p31| p31.as_array()).map_or(
                                    false,
                                    |instances| {
                                        instances.iter().any(|i| {
                                            i["mainsnak"]["datavalue"]["value"]["id"]
                                                == *instance_of
                                        })
                                    },
                                ) {
                                    // Batch the writes
                                    let mut writer = batched_writer.lock().unwrap();
                                    write_entity_data(
                                        &resolver,
                                        &mut writer,
                                        entity_type,
                                        &entity.id,
                                        label,
                                        description,
                                        &claims,
                                        &config,
                                        &default_properties,
                                    )?;
                                }
                            }
                        }
                    }
                }
            }

            Ok(())
        })?;

    // Final flush of any remaining entries
    batched_writer.lock().unwrap().finalize()?;

    // Clear progress line
    println!("\rProcessing: 100% | Completed                        ");

    Ok(())
}

// Lazy static for caching resolved entities
// lazy_static! {
//     static ref ENTITY_CACHE: Mutex<HashMap<String, String>> = Mutex::new(HashMap::new());
// }

// /// Resolves Wikibase entity IDs to their labels
// ///
// /// # Arguments
// ///
// /// * `properties` - A mutable map of properties to be processed
// /// * `api_base_url` - Base URL for the Wikibase API (e.g., "https://www.wikidata.org/w/api.php")
// ///
// /// # Returns
// ///
// /// A modified map of properties with entity IDs replaced by their labels
// fn resolve_entity_ids(mut properties: Map<String, Value>) -> Map<String, Value> {
//     let api_base_url = "https://www.wikidata.org/w/api.php";
//     // Identify entity IDs to resolve
//     let mut ids_to_resolve = HashSet::new();
//     let cache = ENTITY_CACHE.lock().unwrap();

//     for (_, value) in &properties {
//         if let Some(id) = value.as_str() {
//             // Extract the base entity ID (part before '$')
//             let base_id = id.split('$').next().unwrap_or(id);

//             if base_id.starts_with('Q')
//                 && base_id.len() > 1
//                 && base_id.chars().skip(1).all(|c| c.is_ascii_digit())
//             {
//                 if !cache.contains_key(base_id) {
//                     ids_to_resolve.insert(base_id.to_string());
//                 }
//             }
//         }
//     }
//     drop(cache); // Explicitly drop the cache lock
//     let ids_vec: Vec<String> = ids_to_resolve.into_iter().collect();

//     // Resolve entities in batches (Wikidata API has limits)
//     let client = Client::new();
//     let batch_size = 50;

//     for batch in ids_vec.chunks(batch_size) {
//         // Check cache first
//         let mut cache = ENTITY_CACHE.lock().unwrap();

//         // Construct API request for uncached IDs
//         let ids_param = batch.join("|");
//         let response = client
//             .get(api_base_url)
//             .query(&[
//                 ("action", "wbgetentities"),
//                 ("format", "json"),
//                 ("ids", &ids_param),
//                 ("props", "labels"),
//                 ("languages", "en"),
//             ])
//             .send()
//             .expect("Failed to send request");

//         let json: Value = response.json().expect("Failed to parse JSON");

//         // Extract labels
//         if let Some(entities) = json["entities"].as_object() {
//             for (id, entity) in entities {
//                 if let Some(label) = entity["labels"]["en"]["value"].as_str() {
//                     cache.insert(id.clone(), label.to_string());
//                 }
//             }
//         }

//         // Replace entity IDs with labels
//         for (_, value) in properties.iter_mut() {
//             if let Some(id) = value.as_str() {
//                 if let Some(label) = cache.get(id) {
//                     *value = Value::String(label.clone());
//                 }
//             }
//         }
//     }

//     properties
// }

fn write_entity_data(
    resolver: &EntityResolver,
    batched_writer: &mut BatchedWriter,
    entity_type: &str,
    entity_id: &str,
    label: &str,
    description: &str,
    claims: &Map<String, Value>,
    config: &Config,
    default_properties: &HashMap<&str, Vec<&str>>,
) -> Result<(), ProcessingError> {
    // Extract properties
    let properties = resolver.resolve_entity_ids(extract_properties(
        entity_type,
        &Value::Object(claims.clone()),
        config.process_images,
        default_properties,
    ));

    // Write to CSV
    batched_writer.add_csv_entry(
        entity_type.to_string(),
        (label.to_string(), entity_id.to_string()),
    )?;

    if let Some(short_name) = properties.get("P1813") {
        batched_writer.add_csv_entry(
            entity_type.to_string(),
            (short_name.to_string(), entity_id.to_string()),
        )?;
    }

    if let Some(nickname) = properties.get("P1449") {
        batched_writer.add_csv_entry(
            entity_type.to_string(),
            (nickname.to_string(), entity_id.to_string()),
        )?;
    }

    // Prepare KV entry
    let kv_entry = serde_json::json!({
        entity_id: {
            "label": label,
            "description": description,
            "properties": Value::Object(properties)
        }
    });

    // Add to batch
    batched_writer.add_kv_entry(kv_entry)?;

    Ok(())
}

fn extract_properties(
    entity_type: &str,
    claims: &Value,
    process_images: bool,
    default_properties: &HashMap<&str, Vec<&str>>,
) -> Map<String, Value> {
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
                        "P569" | "P570" | "P571" => {
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
                        "P17" | "P112" | "P27" | "P106" | "P39" | "P1454" | "P749" | "P101"
                        | "P452" | "P276" | "P31" | "P585" => {
                            // Handle string or entity-id properties (e.g., country, occupation, position)
                            if let Some(id_value) = value
                                .get("mainsnak")
                                .and_then(|ms| ms.get("datavalue"))
                                .and_then(|dv| dv.get("value"))
                                .and_then(|v| v.get("id"))
                            {
                                properties.insert(prop.to_string(), id_value.clone());
                            }
                        }
                        "P18" | "P154" => {
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
                        "P159" => {
                            // Extract location address or entity
                            if let Some(location) = value
                                .get("mainsnak")
                                .and_then(|ms| ms.get("datavalue"))
                                .and_then(|dv| dv.get("value"))
                            {
                                properties.insert(prop.to_string(), location.clone());
                            }
                        }
                        "P1813" | "P1449" => {
                            // Extract short name or alias
                            if let Some(short_name) = value
                                .get("mainsnak")
                                .and_then(|ms| ms.get("datavalue"))
                                .and_then(|dv| dv.get("value"))
                                .and_then(|v| v.get("text"))
                            {
                                properties.insert(
                                    prop.to_string(),
                                    Value::String(short_name.as_str().unwrap_or("").to_string()),
                                );
                            }
                        }
                        "P856" | "P3220" => {
                            // Extract URLs (P856 = Official website, P3220 = Google Maps ID)
                            if let Some(url) = value
                                .get("mainsnak")
                                .and_then(|ms| ms.get("datavalue"))
                                .and_then(|dv| dv.get("value"))
                                .and_then(|v| v.as_str())
                            {
                                properties.insert(prop.to_string(), Value::String(url.to_string()));
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
    // Value::Object(properties)
    properties
}

fn main() -> Result<(), ProcessingError> {
    // Parse command-line arguments
    let matches = Command::new("Wikidata Entity Extraction")
        .version("1.0")
        .author("Erik Vullings")
        .about("Extracts and processes Wikidata for OSINT analysis")
        .arg(Arg::new("entity_types")
            .short('e')
            .long("entity-types")
            .help("Comma-separated list of entity types to process (e.g., person, organization, location)")
            .default_value("person,organization,scientific_organization,research_institute,government_agency,association,location,event")
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
