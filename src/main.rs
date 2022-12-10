use awscreds::Credentials;
use futures::lock::Mutex;
use futures::stream::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::{path::Path, ObjectStore};
use std::sync::Arc;
use std::time::SystemTime;
mod read_from_object_store;
use read_from_object_store::parallel_read;
mod async_read_from_object_store;
use async_read_from_object_store::parallel_read as async_parallel_read;
use std::env;

const PARQUET: &str = "delta-rs/rust/tests/data/simple_table/part-00190-8ac0ae67-fb1d-461d-a3d3-8dc112766ff5-c000.snappy.parquet";

pub async fn list_bucket(object_store: &Arc<dyn ObjectStore>) {
    // Recursively list all files in the bucket.
    let prefix: Path = "".try_into().unwrap();

    // Get an `async` stream of Metadata objects:
    let list_stream = object_store
        .list(Some(&prefix))
        .await
        .expect("Error listing files");

    // Print a line about each object based on its metadata
    // using for_each from `StreamExt` trait.
    list_stream
        .for_each(move |meta| async {
            let meta = meta.expect("Error listing");
            println!("Name: {}, size: {}", meta.location, meta.size);
        })
        .await;
}

#[tokio::main]
async fn main() {
    // Setup tracing.
    tracing_subscriber::fmt::try_init().unwrap();
    // let subscriber = tracing_subscriber::FmtSubscriber::new();
    // tracing::subscriber::set_global_default(subscriber).unwrap();

    // Create an ObjectStore.

    let cred = Credentials::default().unwrap();
    let s3 = AmazonS3Builder::new()
        .with_access_key_id(cred.access_key.unwrap())
        .with_secret_access_key(cred.secret_key.unwrap())
        .with_region("us-west-2")
        .with_bucket_name("lov2test")
        .build()
        .unwrap();

    // Process cli arguments.
    let args = env::args().collect::<Vec<_>>();
    match args.len() {
        2 => {
            let start = SystemTime::now();
            let arg = &args[1];
            match &arg[..] {
                "list" => {
                    let object_store: Arc<dyn ObjectStore> = Arc::new(s3);
                    list_bucket(&object_store).await
                }
                "ranged" => {
                    let object_store: Arc<dyn ObjectStore> = Arc::new(s3);
                    let path = Path::from(PARQUET);
                    parallel_read(object_store, path, 0).await.unwrap();
                }
                "async" => {
                    let object_store = Arc::new(Mutex::new(s3));
                    let path = Path::from(PARQUET);
                    async_parallel_read(object_store, path, 0).await.unwrap();
                }
                other => {
                    println!("Unknow argument {other}, use one of:\n  list\n  ranged\n  async\n")
                }
            }

            println!("took: {} ms", start.elapsed().unwrap().as_millis());
        }

        num => {
            println!(
                "Unexpected number of arguments {num}, use one of:\n  list\n  ranged\n  async\n"
            )
        }
    }
}
