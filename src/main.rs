use rayon::prelude::*;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod utils;
use utils::{create_image_thumbnail_url, fetch_base64_image};
mod batched_writer;
use batched_writer::BatchedWriter;
mod entity_resolver;
use entity_resolver::EntityResolver;
mod processing_error;
use processing_error::ProcessingError;
mod config;
use config::{get_configuration, Config};

#[derive(Debug, Deserialize)]
struct WikidataEntity {
    id: String,
    claims: Option<Map<String, Value>>,
    labels: Option<Map<String, Value>>,
    descriptions: Option<Map<String, Value>>,
    aliases: Option<Map<String, Value>>,
    // #[serde(default)]
    // sitelinks: Value,
}

fn get_entity_type_mappings() -> HashMap<&'static str, Vec<&'static str>> {
    HashMap::from([
        // human: https://www.wikidata.org/wiki/Q5
        ("person", vec!["Q5"]),
        ("organization", vec!["Q43229"]),
        ("scientific_organization", vec!["Q16519632"]),
        ("research_institute", vec!["Q31855"]),
        ("government_agency", vec!["Q327333"]),
        ("event", vec!["Q1656682"]),
        (
            "mood",
            vec![
                "Q331769",   // mood
                "Q41537118", // emotional state
                "Q3968640",  // mental state
                "Q16748867", // basic emotion
                "Q9415",     // emotions
                "Q9332",     // behavior
                "Q60539479", // positive emotion
                "Q60539481", // negative emotion
            ],
        ),
    ])
}

fn get_default_properties() -> HashMap<&'static str, Vec<&'static str>> {
    let organization_props = vec![
        "P31",   // Instance of
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
            "mood",
            vec![
                "P31",   // Instance of
                "P1552", // Has characteristic
                "P1889", // Different from
                "P461",  // Opposite of
                "P460",  // Said to be the same as
                "P1382", // Partially coincident with
                "P18",   // Image
            ],
        ),
    ])
}

fn process_wikidata(input_path: String, config: Config) -> Result<(), ProcessingError> {
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
    let batched_writer =
        BatchedWriter::new(csv_writers, kv_file, config.output_format.clone(), 50000);
    let batched_writer = Arc::new(Mutex::new(batched_writer));

    // Open input file and get total file size for progress tracking
    let file = File::open(input_path).expect("JSON dump file not found");
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
            // if let Some(title) = entity.sitelinks["enwiki"]["title"].as_str() {
            //     dbg!(title);
            // }

            // Process entity
            if let (Some(claims), Some(labels), Some(descriptions), Some(aliases)) = (
                entity.claims,
                entity.labels,
                entity.descriptions,
                entity.aliases,
            ) {
                let description = descriptions
                    .get(&config.lang)
                    // .or(descriptions.get("en"))
                    .and_then(|obj| obj.get("value"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let aliases = aliases
                    .get(&config.lang)
                    .and_then(|value| value.as_array())
                    .and_then(|values| {
                        // dbg!(&values);
                        Some(
                            values
                                .iter()
                                .map(|v| v.get("value").and_then(|v| v.as_str()).unwrap_or(""))
                                .collect::<Vec<&str>>(), // .join(";"),
                        )
                    })
                    .unwrap_or(Vec::new());
                // if !aliases.is_empty() {
                //     dbg!(&aliases);
                // }
                if let Some(label_obj) = labels.get(&config.lang) {
                    if let Some(label) = label_obj.get("value").and_then(|v| v.as_str()) {
                        for entity_type in &config.entity_types {
                            if let Some(instance_of) = entity_mappings.get(entity_type.as_str()) {
                                if claims.get("P31").and_then(|p31| p31.as_array()).map_or(
                                    false,
                                    |instances| {
                                        instances.iter().any(|i| {
                                            if let Some(instance) =
                                                i["mainsnak"]["datavalue"]["value"]["id"].as_str()
                                            {
                                                instance_of.contains(&instance)
                                            } else {
                                                false
                                            }
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
                                        &aliases,
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

fn write_entity_data(
    resolver: &EntityResolver,
    batched_writer: &mut BatchedWriter,
    entity_type: &str,
    entity_id: &str,
    label: &str,
    description: &str,
    aliases: &Vec<&str>,
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

    if !label.is_empty() {
        // Write to CSV
        batched_writer.add_csv_entry(
            entity_type.to_string(),
            (label.to_string(), entity_id.to_string()),
        )?;

        if let Some(short_name_val) = properties.get("P1813") {
            if let Some(short_name) = short_name_val.as_str() {
                batched_writer.add_csv_entry(
                    entity_type.to_string(),
                    (short_name.to_string(), entity_id.to_string()),
                )?;
            }
        }

        if let Some(nickname_val) = properties.get("P1449") {
            if let Some(nickname) = nickname_val.as_str() {
                batched_writer.add_csv_entry(
                    entity_type.to_string(),
                    (nickname.to_string(), entity_id.to_string()),
                )?;
            }
        }

        if !aliases.is_empty() {
            for alias in aliases {
                batched_writer.add_csv_entry(
                    entity_type.to_string(),
                    (alias.to_string(), entity_id.to_string()),
                )?;
            }
        }

        // Prepare KV entry
        let mut entity_data = serde_json::Map::new();
        entity_data.insert("label".to_string(), json!(label));

        // Conditionally add description if not empty
        if !description.is_empty() {
            entity_data.insert("descr".to_string(), json!(description));
        }

        // Conditionally add aliases if not empty
        if !aliases.is_empty() {
            entity_data.insert("alias".to_string(), json!(aliases));
        }

        // Always add properties
        if properties.len() > 0 {
            entity_data.insert("props".to_string(), json!(properties));
        }

        let kv_entry = json!({
            entity_id: entity_data
        });

        // Add to batch
        batched_writer.add_kv_entry(kv_entry)?;
    }

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
                        | "P452" | "P276" | "P31" | "P585" | "P1552" | "P1889" | "P461"
                        | "P460" | "P1382" => {
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
                            {
                                if process_images {
                                    if let Some(url) = create_image_thumbnail_url(commons_url, None)
                                    {
                                        if let Ok(base64_image) = fetch_base64_image(url) {
                                            properties.insert(
                                                "image".to_string(),
                                                Value::String(base64_image),
                                            );
                                        }
                                    }
                                } else {
                                    properties.insert(
                                        "image".to_string(),
                                        Value::String(commons_url.to_string()),
                                    );
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
            }
        }
        None => {}
    }
    properties
}

fn main() -> Result<(), ProcessingError> {
    let (input_file, config) = get_configuration()?;

    process_wikidata(input_file, config)
}
