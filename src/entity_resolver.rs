use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct EntityCache {
    entries: HashMap<String, String>,
}

impl EntityCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    // Load cache from CSV file
    fn load_from_csv(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        if !path.exists() {
            return Ok(Self::new());
        }

        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let mut entries = HashMap::new();
        let mut rdr = csv::Reader::from_reader(contents.as_bytes());
        for result in rdr.records() {
            let record = result?;
            if record.len() == 2 {
                entries.insert(record[0].to_string(), record[1].to_string());
            }
        }

        Ok(Self { entries })
    }

    // Save cache to CSV file
    fn save_to_csv(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let mut wtr = csv::Writer::from_path(path)?;

        for (id, label) in &self.entries {
            wtr.write_record(&[id, label])?;
        }

        wtr.flush()?;
        Ok(())
    }
}

pub struct EntityResolver {
    cache: Arc<RwLock<EntityCache>>,
    cache_file_path: PathBuf,
    save_counter: Arc<Mutex<usize>>,
    /// Wikibase API url
    api_base_url: String,
    /// Required language
    language: String,
    /// Required language including English in case the required language is not English
    languages: String,
}

impl EntityResolver {
    pub fn new(cache_file_path: PathBuf, api_base_url: String, language: &str) -> Self {
        // Try to load existing cache, or create a new one
        let cache = match EntityCache::load_from_csv(&cache_file_path) {
            Ok(loaded_cache) => Arc::new(RwLock::new(loaded_cache)),
            Err(_) => Arc::new(RwLock::new(EntityCache::new())),
        };

        let languages = if language == "en" {
            "en".to_string()
        } else {
            format!("{},en", language)
        };

        Self {
            cache,
            cache_file_path,
            save_counter: Arc::new(Mutex::new(0)),
            api_base_url,
            language: language.to_string(),
            languages,
        }
    }

    // Check if ID exists in cache without locking
    fn get_cached_label(&self, id: &str) -> Option<String> {
        self.cache.read().unwrap().entries.get(id).cloned()
    }

    // Resolve entities, with optimized locking
    pub fn resolve_entity_ids(&self, mut properties: Map<String, Value>) -> Map<String, Value> {
        // Collect IDs to resolve
        let mut ids_to_resolve = HashSet::new();

        for (_, value) in &properties {
            if let Some(full_id) = value.as_str() {
                // Extract base ID
                let base_id = full_id.split('$').next().unwrap_or(full_id);

                // Check if ID needs resolving
                if base_id.starts_with('Q')
                    && base_id.len() > 1
                    && base_id.chars().skip(1).all(|c| c.is_ascii_digit())
                {
                    // Only add if not in cache
                    if self.get_cached_label(base_id).is_none() {
                        ids_to_resolve.insert(base_id.to_string());
                    }
                }
            }
        }

        // Resolve unknown entities via API
        if !ids_to_resolve.is_empty() {
            self.fetch_and_cache_entities(&ids_to_resolve, &self.api_base_url);
        }

        let mut keys_to_remove = Vec::new();
        // Replace IDs with labels
        for (key, value) in properties.iter_mut() {
            if let Some(full_id) = value.as_str() {
                let base_id = full_id.split('$').next().unwrap_or(full_id);

                if let Some(label) = self.get_cached_label(base_id) {
                    if !label.is_empty() {
                        *value = Value::String(label);
                    } else {
                        // Remove the property if the label is empty
                        keys_to_remove.push(key.clone());
                    }
                }
            }
        }

        // Remove keys after the loop
        for key in keys_to_remove {
            properties.remove(&key);
        }
        properties
    }

    // Fetch and cache entities
    fn fetch_and_cache_entities(&self, ids: &HashSet<String>, api_base_url: &str) {
        let client = Client::new();
        let batch_size = 50;

        // Convert to vec for batching
        let ids_vec: Vec<String> = ids.iter().cloned().collect();

        for batch in ids_vec.chunks(batch_size) {
            // Construct API request
            let ids_param = batch.join("|");
            let response = client
                .get(api_base_url)
                .query(&[
                    ("action", "wbgetentities"),
                    ("format", "json"),
                    ("ids", &ids_param),
                    ("props", "labels"),
                    ("languages", &self.languages),
                ])
                .send()
                .expect("Failed to send request");

            let json: Value = response.json().expect("Failed to parse JSON");

            // Extract and cache labels
            if let Some(entities) = json["entities"].as_object() {
                let mut labels_to_cache = HashMap::new();

                for (id, entity) in entities {
                    if let Some(label) = entity["labels"][&self.language]["value"]
                        .as_str()
                        .or(entity["labels"]["en"]["value"].as_str().or(Some("")))
                    {
                        labels_to_cache.insert(id.clone(), label.to_string());
                    }
                }

                // Batch write to cache
                if !labels_to_cache.is_empty() {
                    self.batch_update_cache(labels_to_cache);
                }
            }
        }
    }

    // Batch update cache with a write lock
    fn batch_update_cache(&self, labels: HashMap<String, String>) {
        let mut save_count = self.save_counter.lock().unwrap();

        // Update cache with write lock
        {
            let mut cache = self.cache.write().unwrap();
            for (id, label) in labels {
                cache.entries.insert(id, label);
            }
        }

        // Periodically save to disk (e.g., every 100 updates)
        *save_count += 1;
        if *save_count % 100 == 0 {
            let cache = self.cache.read().unwrap();
            if let Err(e) = cache.save_to_csv(&self.cache_file_path) {
                eprintln!("Failed to save cache: {}", e);
            }
        }
    }
}

// // Example usage
// fn main() {
//     // Create resolver with a specific cache file path
//     let resolver = EntityResolver::new(PathBuf::from("entity_cache.csv"));

//     // Sample properties
//     let mut properties = HashMap::from([
//         ("P106".to_string(), Value::String("Q10841764".to_string())),
//         (
//             "name".to_string(),
//             Value::String("Lewis Hamilton".to_string()),
//         ),
//     ]);

//     // Resolve entities
//     let resolved_properties =
//         resolver.resolve_entity_ids(properties, "https://www.wikidata.org/w/api.php");
// }
