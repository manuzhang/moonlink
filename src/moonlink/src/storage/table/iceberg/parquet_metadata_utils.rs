use crate::storage::table::iceberg::compat;

use bytes::Bytes;
use futures::future::BoxFuture;
use iceberg::{io::FileMetadata, io::FileRead, io::InputFile, Result as IcebergResult};
use parquet::arrow::async_reader::MetadataFetch;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

struct IcebergMetadataFetch {
    reader: Box<dyn FileRead>,
}

impl MetadataFetch for IcebergMetadataFetch {
    fn fetch(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let reader = self.reader.as_ref();
        Box::pin(async move {
            reader
                .read(range)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        })
    }
}

/// Get parquet metadata from the given file.
pub(crate) async fn get_parquet_metadata(
    file_metadata: FileMetadata,
    input_file: InputFile,
) -> IcebergResult<ParquetMetaData> {
    let file_size_in_bytes = file_metadata.size;
    let reader = input_file.reader().await?;
    let metadata_fetch = IcebergMetadataFetch { reader };

    // TODO(hjiang): Check IO operation number and decide reader options.
    // As of now it's only accessing local files and will cached by filesystem.
    let parquet_meta_data_reader = ParquetMetaDataReader::new()
        .with_prefetch_hint(None)
        .with_column_index_policy(PageIndexPolicy::Optional)
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_offset_index_policy(PageIndexPolicy::Optional);
    let parquet_metadata = parquet_meta_data_reader
        .load_and_finish(metadata_fetch, file_size_in_bytes)
        .await
        .map_err(compat::parquet_error_to_iceberg)?;

    Ok(parquet_metadata)
}
