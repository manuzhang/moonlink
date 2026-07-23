use arrow_schema::Schema as ArrowSchema;
use arrow_schema_58::Schema as IcebergArrowSchema;
use iceberg::arrow as IcebergArrow;
use iceberg::spec::Schema as IcebergSchema;
use iceberg::{Error, ErrorKind, Result as IcebergResult};
use parquet::errors::ParquetError;

pub(crate) fn arrow_schema_to_schema(schema: &ArrowSchema) -> IcebergResult<IcebergSchema> {
    let iceberg_arrow_schema = serde_json::from_value::<IcebergArrowSchema>(
        serde_json::to_value(schema).map_err(schema_conversion_error)?,
    )
    .map_err(schema_conversion_error)?;
    IcebergArrow::arrow_schema_to_schema(&iceberg_arrow_schema)
}

pub(crate) fn schema_to_arrow_schema(schema: &IcebergSchema) -> IcebergResult<ArrowSchema> {
    let iceberg_arrow_schema = IcebergArrow::schema_to_arrow_schema(schema)?;
    serde_json::from_value(
        serde_json::to_value(&iceberg_arrow_schema).map_err(schema_conversion_error)?,
    )
    .map_err(schema_conversion_error)
}

pub(crate) fn parquet_error_to_iceberg(error: ParquetError) -> Error {
    Error::new(ErrorKind::DataInvalid, "Error reading Parquet data").with_source(error)
}

#[cfg(test)]
pub(crate) fn arrow_error_to_iceberg(error: arrow_schema::ArrowError) -> Error {
    Error::new(ErrorKind::DataInvalid, "Error reading Arrow data").with_source(error)
}

fn schema_conversion_error(error: serde_json::Error) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        "Error converting Arrow schema across Iceberg boundary",
    )
    .with_source(error)
}
