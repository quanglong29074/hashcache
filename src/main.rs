use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, Result, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

type SharedStore = Arc<RwLock<HashMap<String, String>>>;

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    store: SharedStore,
) -> Result<Response<Full<Bytes>>> {
    let path = req.uri().path().to_string();

    match (req.method(), path.as_str()) {
        (&Method::PUT, path) if path.starts_with('/') => {
            let key = path[1..].to_string(); // Remove leading '/'

            if key.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Key cannot be empty")))
                    .unwrap());
            }

            // Read the request body
            let body_bytes = match req.collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(_) => {
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Full::new(Bytes::from("Failed to read body")))
                        .unwrap());
                }
            };

            let value = String::from_utf8_lossy(&body_bytes).to_string();

            // Store the key-value pair
            {
                let mut store = store.write().await;
                store.insert(key.clone(), value);
            }

            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from(format!("Stored key: {}", key))))
                .unwrap())
        }

        (&Method::GET, path) if path.starts_with('/') => {
            let key = path[1..].to_string(); // Remove leading '/'

            if key.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("Key cannot be empty")))
                    .unwrap());
            }

            // Retrieve the value
            let store = store.read().await;
            match store.get(&key) {
                Some(value) => Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(Full::new(Bytes::from(value.clone())))
                    .unwrap()),
                None => Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::from("Key not found")))
                    .unwrap()),
            }
        }

        (&Method::GET, "/stats") => {
            let store = store.read().await;
            let count = store.len();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(format!(
                    "{{\"total_keys\": {}}}",
                    count
                ))))
                .unwrap())
        }

        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap()),
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await?;

    // Create shared HashMap store
    let store: SharedStore = Arc::new(RwLock::new(HashMap::new()));

    println!("HashCache server running on http://{}", addr);
    println!("Usage:");
    println!("  PUT: curl -X PUT http://localhost:3000/mykey --data-binary \"myvalue\"");
    println!("  GET: curl http://localhost:3000/mykey");
    println!("  Stats: curl http://localhost:3000/stats");

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let store_clone = Arc::clone(&store);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| handle_request(req, Arc::clone(&store_clone))),
                )
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
