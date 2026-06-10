use crate::error::{Error, Result};
use crate::table_config::TableConfig;
use moonlink::row::IdentityProp;
use moonlink::{MooncakeTableId, MoonlinkTableConfig};

/// Util functions for parse configurations.
///
/// Parse event table config from serialization string, and fill in default value if unassigned.
pub(crate) fn parse_event_table_config(
    moonlink_table_config: &str,
    mooncake_table_id: &MooncakeTableId,
    table_base_path: &str,
    temp_files_dir: &str,
) -> Result<MoonlinkTableConfig> {
    let mut table_config =
        TableConfig::from_json_or_default(moonlink_table_config, table_base_path)?;

    // If table config is already valid, directly transform to moonlink config and return.
    if table_config.is_valid() {
        return table_config.take_as_moonlink_config(temp_files_dir.to_string(), mooncake_table_id);
    }

    // Otherwise manually set based on event table native properties.
    let mooncake_config = &mut table_config.mooncake_config;

    // If user provided config is invalid already, return error.
    if let (Some(is_append_only), Some(row_identity)) = (
        mooncake_config.append_only,
        mooncake_config.row_identity.as_ref(),
    ) {
        let is_none_identity = *row_identity == IdentityProp::None;
        if is_append_only != is_none_identity {
            return Err(Error::invalid_config(
                "Append only table shouldn't have identity property".to_string(),
            ));
        }
    }

    // If part of the table properties is unassigned, backfill with default value.
    if mooncake_config.append_only == Some(true) && mooncake_config.row_identity.is_none() {
        mooncake_config.row_identity = Some(IdentityProp::None);
    } else if mooncake_config
        .row_identity
        .as_ref()
        .is_some_and(|row_identity| *row_identity == IdentityProp::None)
        && mooncake_config.append_only.is_none()
    {
        mooncake_config.append_only = Some(true);
    }
    table_config.take_as_moonlink_config(temp_files_dir.to_string(), mooncake_table_id)
}

/// Parse replication table config, and fill in default value if unassigned.
pub(crate) fn parse_replication_table_config(
    moonlink_table_config: &str,
    mooncake_table_id: &MooncakeTableId,
    table_base_path: &str,
    temp_files_dir: &str,
) -> Result<MoonlinkTableConfig> {
    let mut table_config =
        TableConfig::from_json_or_default(moonlink_table_config, table_base_path)?;
    table_config.mooncake_config.row_identity = Some(IdentityProp::FullRow);
    table_config.mooncake_config.append_only = Some(false);
    table_config.take_as_moonlink_config(temp_files_dir.to_string(), mooncake_table_id)
}
