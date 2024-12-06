use base64::{engine::general_purpose, Engine};
use md5::{Digest, Md5};
use rand::Rng;
use reqwest::{
    blocking::Client,
    header::{
        HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, REFERER, USER_AGENT,
    },
};

/// Convert the image URL to a full URL using the MD5 hash of the name
pub fn create_image_thumbnail_url(filename: &str, width: Option<u32>) -> Option<String> {
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

/// Generate random headers for browser requests
pub fn generate_browser_headers() -> HeaderMap {
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

/// Fetch image from Wikipedia and encode as base64
pub fn fetch_base64_image(commons_url: String) -> Result<String, reqwest::Error> {
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
