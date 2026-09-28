use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::storage_config::StorageConfig;
use crate::storage::filesystem::test_utils::object_storage_test_utils::*;
use crate::storage::filesystem::{
    accessor::base_filesystem_accessor::BaseFileSystemAccess, accessor_config::AccessorConfig,
};

use std::sync::Arc;
use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, SignableBody, SignableRequest, SigningSettings,
};
use aws_sigv4::sign::v4::SigningParams;
use iceberg::{Error as IcebergError, Result as IcebergResult};
use std::time::SystemTime;

use backon::{ExponentialBuilder, Retryable};
use tokio::time::sleep;

/// S3-compatible test storage constants.
///
/// Local minio warehouse needs special handling, so we simply prefix with special token.
pub(crate) static S3_TEST_BUCKET_PREFIX: &str = "test-minio-warehouse-";
pub(crate) static S3_TEST_WAREHOUSE_URI_PREFIX: &str = "s3://test-minio-warehouse-";
pub(crate) static S3_TEST_ACCESS_KEY_ID: &str = "minioadmin";
pub(crate) static S3_TEST_SECRET_ACCESS_KEY: &str = "minioadmin";
pub(crate) static S3_TEST_ENDPOINT: &str = "http://s3.local:9000";
pub(crate) static S3_TEST_REGION: &str = "us-east-1";

/// Create a S3 catalog config.
pub(crate) fn create_s3_storage_config(warehouse_uri: &str) -> AccessorConfig {
    let bucket = get_bucket_from_warehouse_uri(warehouse_uri);
    let storage_config = StorageConfig::S3 {
        access_key_id: S3_TEST_ACCESS_KEY_ID.to_string(),
        secret_access_key: S3_TEST_SECRET_ACCESS_KEY.to_string(),
        region: S3_TEST_REGION.to_string(), // minio doesn't care about region.
        bucket: bucket.to_string(),
        endpoint: Some(S3_TEST_ENDPOINT.to_string()),
    };
    AccessorConfig::new_with_storage_config(storage_config)
}

pub(crate) fn get_test_s3_bucket_and_warehouse() -> (String, String) {
    get_bucket_and_warehouse(S3_TEST_BUCKET_PREFIX, S3_TEST_WAREHOUSE_URI_PREFIX)
}

async fn create_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
    s3_bucket_request("PUT", &bucket).await?;

    Ok(())
}

async fn delete_s3_bucket_objects(bucket: &str) -> IcebergResult<()> {
    let config = create_s3_storage_config(&format!("s3://{bucket}"));
    let accessor = FileSystemAccessor::new(config);
    accessor.remove_directory("/").await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to remove directory in bucket {bucket}: {e}"),
        )
    })?;
    Ok(())
}

async fn delete_test_s3_bucket_impl(bucket: Arc<String>) -> IcebergResult<()> {
    // Delete all objects in the bucket first.
    delete_s3_bucket_objects(&bucket).await?;

    s3_bucket_request("DELETE", &bucket).await?;

    Ok(())
}

async fn s3_bucket_request(method: &str, bucket: &str) -> IcebergResult<()> {
    let url = format!("{S3_TEST_ENDPOINT}/{bucket}");
    let credentials = Credentials::new(
        S3_TEST_ACCESS_KEY_ID,
        S3_TEST_SECRET_ACCESS_KEY,
        None,
        None,
        "moonlink-test",
    );
    let identity = credentials.into();
    let mut signing_settings = SigningSettings::default();
    signing_settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    let params = SigningParams::builder()
        .identity(&identity)
        .region(S3_TEST_REGION)
        .name("s3")
        .time(SystemTime::now())
        .settings(signing_settings)
        .build()
        .map_err(|e| IcebergError::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;
    let signable = SignableRequest::new(method, &url, std::iter::empty(), SignableBody::Bytes(&[]))
        .map_err(|e| IcebergError::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;
    let (instructions, _) = sign(signable, &params.into())
        .map_err(|e| IcebergError::new(iceberg::ErrorKind::Unexpected, e.to_string()))?
        .into_parts();
    let client = reqwest::Client::new();
    let mut request_builder = client.request(
        reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
        &url,
    );
    for (name, value) in instructions.headers() {
        request_builder = request_builder.header(name, value);
    }
    let request = request_builder
        .build()
        .map_err(|e| IcebergError::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;
    let response = client
        .execute(request)
        .await
        .map_err(|e| IcebergError::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;
    if !response.status().is_success() {
        return Err(IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!(
                "S3 {method} bucket request failed with status {}",
                response.status()
            ),
        ));
    }
    Ok(())
}

/// Creates the provided bucket with exponential backoff retry; this function assumes the bucket doesn't exist, otherwise it will return error.
pub(crate) async fn create_test_s3_bucket(bucket: String) -> IcebergResult<()> {
    let bucket = Arc::new(bucket);
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(TEST_RETRY_INIT_MILLISEC))
        .with_max_times(TEST_RETRY_COUNT);

    (move || {
        let bucket = Arc::clone(&bucket);
        async move { create_test_s3_bucket_impl(bucket).await }
    })
    .retry(backoff)
    .sleep(sleep)
    .when(|e: &IcebergError| {
        matches!(
            e.kind(),
            iceberg::ErrorKind::Unexpected | iceberg::ErrorKind::CatalogCommitConflicts
        )
    })
    .await
}

pub(crate) async fn delete_test_s3_bucket(bucket: String) {
    let bucket = Arc::new(bucket);
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(TEST_RETRY_INIT_MILLISEC))
        .with_max_times(TEST_RETRY_COUNT);

    let _ = (move || {
        let bucket = Arc::clone(&bucket);
        async move { delete_test_s3_bucket_impl(bucket).await }
    })
    .retry(backoff)
    .sleep(sleep)
    .when(|e: &IcebergError| {
        matches!(
            e.kind(),
            iceberg::ErrorKind::Unexpected | iceberg::ErrorKind::CatalogCommitConflicts
        )
    })
    .await;
}
