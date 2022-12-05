use awscreds::Credentials;
use futures::stream::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::{path::Path, ObjectStore};
use std::sync::Arc;
use std::time::SystemTime;
mod read_from_object_store;
use read_from_object_store::parallel_read;

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
    // create an ObjectStore

    let cred = Credentials::default().unwrap();

    let s3 = AmazonS3Builder::new()
        .with_access_key_id(cred.access_key.unwrap())
        .with_secret_access_key(cred.secret_key.unwrap())
        .with_region("us-west-2")
        .with_bucket_name("lov2test")
        .build()
        .unwrap();

    let object_store: Arc<dyn ObjectStore> = Arc::new(s3);

    let start = SystemTime::now();
    let path = Path::from(PARQUET);
    parallel_read(object_store, path, 0).await.unwrap();
    println!("took: {} ms", start.elapsed().unwrap().as_millis());
}
