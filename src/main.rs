use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, Result, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Region};
use sled::Db;

// Cargo.toml - same as Phase 2 plus:
// [dependencies]
// # ... previous ...
// bytes = "1.5"

const MAGIC_NUMBER: u64 = 0x5343524942;
const SEGMENT_VERSION: u32 = 1;
const FLUSH_THRESHOLD: usize = 100;
const FLUSH_INTERVAL_SECS: u64 = 300;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SegmentHeader {
    magic: u64,
    version: u32,
    record_count: u64,
    timestamp: i64,
    min_key: String,
    max_key: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SegmentMetadata {
    id: String,
    s3_key: String,
    record_count: usize,
    min_key: String,
    max_key: String,
    created_at: i64,
    size_bytes: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Manifest {
    version: u64,
    segments: Vec<SegmentMetadata>,
    last_updated: i64,
}

impl Manifest {
    fn new() -> Self {
        Self {
            version: 0,
            segments: Vec::new(),
            last_updated: Self::now(),
        }
    }

    fn add_segment(&mut self, segment: SegmentMetadata) {
        self.segments.push(segment);
        self.version += 1;
        self.last_updated = Self::now();
    }

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    // NEW: Find segments that might contain a key
    fn find_segments_for_key(&self, key: &str) -> Vec<&SegmentMetadata> {
        self.segments
            .iter()
            .filter(|seg| key >= seg.min_key.as_str() && key <= seg.max_key.as_str())
            .collect()
    }
}

struct SegmentFlusher {
    db: Arc<Db>,
    s3_client: S3Client,
    bucket: String,
    manifest: Arc<RwLock<Manifest>>,
    flush_threshold: usize,
}

impl SegmentFlusher {
    async fn new(db: Arc<Db>, bucket: String, flush_threshold: usize) -> Self {
        let s3_client = Self::build_s3_client().await;
        let manifest = Arc::new(RwLock::new(
            Self::load_manifest(&s3_client, &bucket).await
                .unwrap_or_else(|| Manifest::new())
        ));

        Self {
            db,
            s3_client,
            bucket,
            manifest,
            flush_threshold,
        }
    }

    async fn build_s3_client() -> S3Client {
        let endpoint_url = std::env::var("AWS_ENDPOINT_URL").ok();
        let config_builder = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let mut s3_config_builder = S3ConfigBuilder::from(&config_builder);

        if let Some(endpoint) = endpoint_url {
            println!("🔧 Using custom S3 endpoint: {}", endpoint);
            s3_config_builder = s3_config_builder
                .endpoint_url(&endpoint)
                .force_path_style(true);
        }

        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_string());
        
        s3_config_builder = s3_config_builder.region(Region::new(region));
        S3Client::from_conf(s3_config_builder.build())
    }

    async fn load_manifest(s3_client: &S3Client, bucket: &str) -> Option<Manifest> {
        let result = s3_client
            .get_object()
            .bucket(bucket)
            .key("manifest.json")
            .send()
            .await
            .ok()?;

        let bytes = result.body.collect().await.ok()?.into_bytes();
        serde_json::from_slice(&bytes).ok()
    }

    async fn save_manifest(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let manifest = self.manifest.read().await;
        let json = serde_json::to_vec_pretty(&*manifest)?;

        self.s3_client
            .put_object()
            .bucket(&self.bucket)
            .key("manifest.json")
            .body(ByteStream::from(json))
            .content_type("application/json")
            .send()
            .await?;

        Ok(())
    }

    async fn start_background_flusher(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_secs(FLUSH_INTERVAL_SECS));

        loop {
            interval.tick().await;

            let key_count = self.db.len();
            println!("📊 Current keys in Sled: {}", key_count);

            if key_count >= self.flush_threshold {
                println!("🚀 Flush threshold reached ({} keys), starting flush...", key_count);
                match self.flush_to_s3().await {
                    Ok(segment_id) => println!("✅ Successfully flushed segment: {}", segment_id),
                    Err(e) => eprintln!("❌ Flush error: {}", e),
                }
            }
        }
    }

    async fn flush_to_s3(&self) -> std::result::Result<String, Box<dyn std::error::Error>> {
        println!("📦 Starting segment flush...");

        let mut records: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for item in self.db.iter() {
            let (key, value) = item?;
            records.push((key.to_vec(), value.to_vec()));
        }

        if records.is_empty() {
            return Err("No data to flush".into());
        }

        records.sort_by(|a, b| a.0.cmp(&b.0));

        let record_count = records.len();
        let min_key = String::from_utf8_lossy(&records[0].0).to_string();
        let max_key = String::from_utf8_lossy(&records[record_count - 1].0).to_string();

        println!("📝 Sorted {} records ({}...{})", 
                 record_count, 
                 &min_key[..min_key.len().min(20)], 
                 &max_key[..max_key.len().min(20)]);

        let segment_data = self.build_segment_binary(&records)?;
        let segment_size = segment_data.len();

        let timestamp = Manifest::now();
        let segment_id = format!("segment_{}", timestamp);
        let s3_key = format!("segments/{}.bin", segment_id);

        println!("☁️  Uploading {} bytes to s3://{}/{}", 
                 segment_size, self.bucket, s3_key);

        self.s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(&s3_key)
            .body(ByteStream::from(segment_data))
            .content_type("application/octet-stream")
            .send()
            .await?;

        let metadata = SegmentMetadata {
            id: segment_id.clone(),
            s3_key: s3_key.clone(),
            record_count,
            min_key,
            max_key,
            created_at: timestamp,
            size_bytes: segment_size,
        };

        {
            let mut manifest = self.manifest.write().await;
            manifest.add_segment(metadata);
        }

        self.save_manifest().await?;

        println!("✨ Segment {} uploaded and manifest updated", segment_id);
        Ok(segment_id)
    }

    fn build_segment_binary(&self, records: &[(Vec<u8>, Vec<u8>)]) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut buffer = Vec::new();

        let header = SegmentHeader {
            magic: MAGIC_NUMBER,
            version: SEGMENT_VERSION,
            record_count: records.len() as u64,
            timestamp: Manifest::now(),
            min_key: String::from_utf8_lossy(&records[0].0).to_string(),
            max_key: String::from_utf8_lossy(&records[records.len() - 1].0).to_string(),
        };

        let header_bytes = bincode::serialize(&header)?;
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&header_bytes);

        for (key, value) in records {
            buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buffer.extend_from_slice(key);
            buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buffer.extend_from_slice(value);
        }

        Ok(buffer)
    }

    // NEW: Search for key in S3 segments
    async fn search_s3_for_key(&self, key: &str) -> Option<Vec<u8>> {
        let manifest = self.manifest.read().await;
        
        // Find relevant segments
        let segments = manifest.find_segments_for_key(key);
        
        if segments.is_empty() {
            println!("🔍 No segments contain key range for: {}", key);
            return None;
        }

        println!("🔍 Searching {} segment(s) for key: {}", segments.len(), key);

        // Search segments from newest to oldest
        for segment in segments.iter().rev() {
            println!("  → Checking segment: {}", segment.id);
            
            if let Some(value) = self.search_segment(segment, key).await {
                println!("  ✅ Found in segment: {}", segment.id);
                
                // Cache back to Sled for faster future access
                let _ = self.db.insert(key.as_bytes(), value.as_slice());
                let _ = self.db.flush_async().await;
                
                return Some(value);
            }
        }

        println!("  ❌ Key not found in any segment");
        None
    }

    // NEW: Search within a single segment
    async fn search_segment(&self, segment: &SegmentMetadata, key: &str) -> Option<Vec<u8>> {
        // Download segment from S3
        let result = self.s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(&segment.s3_key)
            .send()
            .await
            .ok()?;

        let data = result.body.collect().await.ok()?.into_bytes();
        
        // Parse and search segment
        self.parse_and_search_segment(&data, key)
    }

    // NEW: Parse segment binary and search for key
    fn parse_and_search_segment(&self, data: &[u8], search_key: &str) -> Option<Vec<u8>> {
        let mut offset = 0;

        // Read header length
        if data.len() < 4 {
            return None;
        }
        let header_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        offset += 4;

        // Skip header
        if data.len() < offset + header_len {
            return None;
        }
        offset += header_len;

        // Binary search through records (since they're sorted)
        let mut records: Vec<(String, Vec<u8>)> = Vec::new();

        // First pass: collect all records
        while offset < data.len() {
            // Read key length
            if data.len() < offset + 4 {
                break;
            }
            let key_len = u32::from_le_bytes([
                data[offset], 
                data[offset + 1], 
                data[offset + 2], 
                data[offset + 3]
            ]) as usize;
            offset += 4;

            // Read key
            if data.len() < offset + key_len {
                break;
            }
            let key = String::from_utf8_lossy(&data[offset..offset + key_len]).to_string();
            offset += key_len;

            // Read value length
            if data.len() < offset + 4 {
                break;
            }
            let value_len = u32::from_le_bytes([
                data[offset], 
                data[offset + 1], 
                data[offset + 2], 
                data[offset + 3]
            ]) as usize;
            offset += 4;

            // Read value
            if data.len() < offset + value_len {
                break;
            }
            let value = data[offset..offset + value_len].to_vec();
            offset += value_len;

            records.push((key, value));
        }

        // Binary search for the key
        records.binary_search_by(|probe| probe.0.as_str().cmp(search_key))
            .ok()
            .map(|idx| records[idx].1.clone())
    }
}

type SharedStore = Arc<Db>;
type SharedFlusher = Arc<SegmentFlusher>;

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    store: SharedStore,
    flusher: SharedFlusher,
) -> Result<Response<Full<Bytes>>> {
    let path = req.uri().path().to_string();

    match (req.method(), path.as_str()) {
        (&Method::PUT, path) if path.starts_with('/') => {
            let key = path[1..].to_string();
            if key.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Key cannot be empty")))
                    .unwrap());
            }

            let body_bytes = match req.collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(_) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Full::new(Bytes::from("Failed to read body")))
                        .unwrap());
                }
            };

            match store.insert(key.as_bytes(), body_bytes.as_ref()) {
                Ok(_) => {
                    let _ = store.flush_async().await;
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::from(format!(
                            "✅ Stored: {} ({} bytes)",
                            key,
                            body_bytes.len()
                        ))))
                        .unwrap())
                }
                Err(e) => Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Full::new(Bytes::from(format!("Error: {}", e))))
                    .unwrap()),
            }
        }

        // UPDATED GET: Now searches S3 if not in local cache
        (&Method::GET, path) if path.starts_with('/') && !path.starts_with("/stats") && !path.starts_with("/manifest") && !path.starts_with("/health") => {
            let key = path[1..].to_string();
            if key.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Key cannot be empty")))
                    .unwrap());
            }

            // 1. Check local Sled cache first
            match store.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    println!("🎯 Cache HIT: {}", key);
                    return Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("X-Cache", "HIT")
                        .body(Full::new(Bytes::copy_from_slice(&value)))
                        .unwrap());
                }
                Ok(None) => {
                    println!("💨 Cache MISS: {}", key);
                    // 2. Search S3 segments
                    if let Some(value) = flusher.search_s3_for_key(&key).await {
                        return Ok(Response::builder()
                            .status(StatusCode::OK)
                            .header("X-Cache", "MISS-S3")
                            .body(Full::new(Bytes::from(value)))
                            .unwrap());
                    }
                    
                    // 3. Not found anywhere
                    return Ok(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Full::new(Bytes::from("Key not found")))
                        .unwrap());
                }
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Full::new(Bytes::from(format!("Error: {}", e))))
                        .unwrap());
                }
            }
        }

        (&Method::GET, "/stats") => {
            let count = store.len();
            let size = store.size_on_disk().unwrap_or(0);
            let manifest = flusher.manifest.read().await;
            let total_segment_records: usize = manifest.segments.iter().map(|s| s.record_count).sum();

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(format!(
                    "{{\"local_keys\": {}, \"disk_bytes\": {}, \"segments\": {}, \"segment_records\": {}, \"manifest_version\": {}}}",
                    count, size, manifest.segments.len(), total_segment_records, manifest.version
                ))))
                .unwrap())
        }

        (&Method::GET, "/manifest") => {
            let manifest = flusher.manifest.read().await;
            let json = serde_json::to_string_pretty(&*manifest).unwrap();

            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(json)))
                .unwrap())
        }

        (&Method::GET, "/health") => {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from("OK")))
                .unwrap())
        }

        (&Method::POST, "/flush") => {
            match flusher.flush_to_s3().await {
                Ok(segment_id) => Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(Full::new(Bytes::from(format!(
                        "✅ Flushed segment: {}",
                        segment_id
                    ))))
                    .unwrap()),
                Err(e) => Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Full::new(Bytes::from(format!("❌ Flush failed: {}", e))))
                    .unwrap()),
            }
        }

        // NEW: Clear local cache (for testing S3 reads)
        (&Method::POST, "/clear-cache") => {
            let cleared = store.clear().is_ok();
            let _ = store.flush_async().await;
            
            if cleared {
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(Full::new(Bytes::from("✅ Local cache cleared")))
                    .unwrap())
            } else {
                Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Full::new(Bytes::from("❌ Failed to clear cache")))
                    .unwrap())
            }
        }

        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap()),
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv::dotenv().ok();

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await?;

    let bucket = std::env::var("S3_BUCKET")
        .unwrap_or_else(|_| "hyra-scribe-ledger".to_string());
    let flush_threshold = std::env::var("FLUSH_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(FLUSH_THRESHOLD);

    let db_path = "./scribe_data";
    let store: SharedStore = Arc::new(sled::open(db_path)?);

    let flusher = Arc::new(
        SegmentFlusher::new(
            Arc::clone(&store),
            bucket.clone(),
            flush_threshold,
        )
        .await,
    );

    let flusher_clone = Arc::clone(&flusher);
    tokio::spawn(async move {
        flusher_clone.start_background_flusher().await;
    });

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  🚀 Hyra Scribe Ledger - Phase 3 (S3 Read Path)          ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("📡 Server: http://{}", addr);
    println!("🪣 S3 Bucket: {}", bucket);
    println!("⚙️  Flush Threshold: {} keys", flush_threshold);
    println!();
    println!("📖 API Endpoints:");
    println!("  PUT    /{{key}}        - Store key-value");
    println!("  GET    /{{key}}        - Retrieve (cache → S3)");
    println!("  GET    /stats         - System statistics");
    println!("  GET    /manifest      - View segment manifest");
    println!("  GET    /health        - Health check");
    println!("  POST   /flush         - Manual segment flush");
    println!("  POST   /clear-cache   - Clear local cache (test S3 reads)");
    println!();
    println!("✨ Features:");
    println!("  ✓ Local Sled cache (fast reads)");
    println!("  ✓ S3 fallback (cold storage)");
    println!("  ✓ Binary search in segments");
    println!("  ✓ Auto cache warming");
    println!("  ✓ X-Cache header (HIT/MISS)");
    println!();

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let store_clone = Arc::clone(&store);
        let flusher_clone = Arc::clone(&flusher);

        tokio::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        handle_request(
                            req,
                            Arc::clone(&store_clone),
                            Arc::clone(&flusher_clone),
                        )
                    }),
                )
                .await
            {
                eprintln!("Connection error: {:?}", err);
            }
        });
    }
}