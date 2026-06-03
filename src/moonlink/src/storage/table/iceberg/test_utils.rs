use crate::storage::table::iceberg::compat;

use arrow_array::RecordBatch;
use iceberg::io::FileIO;
use iceberg::Result as IcebergResult;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Test util function to load the first arrow batch from the given parquet file.
/// Precondition: caller unit tests persist rows in one arrow record batch and one parquet file.
pub(crate) async fn load_arrow_batch(
    file_io: &FileIO,
    filepath: &str,
) -> IcebergResult<RecordBatch> {
    let input_file = file_io.new_input(filepath)?;
    let input_file_metadata = input_file.metadata().await?;
    let reader = input_file.reader().await?;
    let bytes = reader.read(0..input_file_metadata.size).await?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(compat::parquet_error_to_iceberg)?;
    let mut reader = builder.build().map_err(compat::parquet_error_to_iceberg)?;
    let batch = reader
        .next()
        .transpose()
        .map_err(compat::arrow_error_to_iceberg)?
        .expect("Should have one batch");
    Ok(batch)
}
