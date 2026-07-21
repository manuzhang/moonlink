// iceberg-rust currently doesn't support puffin related features, to write deletion vector into iceberg metadata, we need two things at least:
// 1. the start offset and blob size for each deletion vector
// 2. append blob metadata into manifest file
// So here to workaround the limitation and to avoid/reduce changes to iceberg-rust ourselves, we use a proxy type to read the writer's metadata before closing it.
//
// deletion vector spec:
// issue collection: https://github.com/apache/iceberg/issues/11122
// deletion vector table spec: https://github.com/apache/iceberg/pull/11240
//
// puffin blob spec: https://iceberg.apache.org/puffin-spec/?h=deletion#deletion-vector-v1-blob-type
//
// TODO(hjiang): Add documentation on how we store puffin blobs inside of puffinf file, what's the relationship between puffin file and manifest file, etc.

use crate::storage::table::iceberg::manifest_utils::{self, ManifestEntryType};

use std::collections::{HashMap, HashSet};

use crate::storage::table::iceberg::data_file_manifest_manager::DataFileManifestManager;
use crate::storage::table::iceberg::deletion_vector_manifest_manager::DeletionVectorManifestManager;
use crate::storage::table::iceberg::file_index_manifest_manager::FileIndexManifestManager;
use iceberg::io::FileIO;
use iceberg::puffin::{CompressionCodec, PuffinWriter};
use iceberg::spec::{FormatVersion, ManifestList, ManifestListWriter, Snapshot, TableMetadata};
use iceberg::Result as IcebergResult;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[allow(dead_code)]
enum PuffinFlagProxy {
    FooterPayloadCompressed = 0,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct PuffinBlobMetadataProxy {
    pub(crate) r#type: String,
    pub(crate) fields: Vec<i32>,
    pub(crate) snapshot_id: i64,
    pub(crate) sequence_number: i64,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) compression_codec: CompressionCodec,
    pub(crate) properties: HashMap<String, String>,
}

#[allow(dead_code)]
struct PuffinWriterProxy {
    writer: Box<dyn iceberg::io::FileWrite>,
    is_header_written: bool,
    num_bytes_written: u64,
    written_blobs_metadata: Vec<PuffinBlobMetadataProxy>,
    properties: HashMap<String, String>,
    footer_compression_codec: CompressionCodec,
    flags: std::collections::HashSet<PuffinFlagProxy>,
}

/// Get puffin blob metadata within the puffin write, and close the writer.
/// This function is supposed to be called after all blobs added.
pub(crate) async fn get_puffin_metadata_and_close(
    puffin_writer: PuffinWriter,
) -> IcebergResult<Vec<PuffinBlobMetadataProxy>> {
    let puffin_writer_proxy =
        unsafe { std::mem::transmute::<PuffinWriter, PuffinWriterProxy>(puffin_writer) };
    let puffin_metadata = puffin_writer_proxy.written_blobs_metadata.clone();
    let puffin_writer =
        unsafe { std::mem::transmute::<PuffinWriterProxy, PuffinWriter>(puffin_writer_proxy) };
    puffin_writer.close().await?;
    Ok(puffin_metadata)
}

/// Util function to create manifest list writer and delete current one.
async fn create_new_manifest_list_writer(
    table_metadata: &TableMetadata,
    cur_snapshot: &Snapshot,
    file_io: &FileIO,
) -> IcebergResult<ManifestListWriter> {
    // Overwrite the old manifest list file.
    let manifest_list_writer = file_io
        .new_output(cur_snapshot.manifest_list())?
        .writer()
        .await?;

    let latest_seq_no = table_metadata.last_sequence_number();
    let manifest_list_writer = if table_metadata.format_version() == FormatVersion::V1 {
        ManifestListWriter::v1(
            manifest_list_writer,
            cur_snapshot.snapshot_id(),
            /*parent_snapshot_id=*/ None,
        )
    } else {
        ManifestListWriter::v2(
            manifest_list_writer,
            cur_snapshot.snapshot_id(),
            /*parent_snapshot_id=*/ None,
            latest_seq_no,
        )
    };
    Ok(manifest_list_writer)
}

/// Get all manifest files and entries,
/// - Data file entries: retain all entries except those marked for removal due to compaction.
/// - Deletion vector entries: remove entries referencing data files to be removed, and merge retained deletion vectors with the provided puffin deletion vector blob.
/// - File indices entries: retain all entries except those marked for removal due to index merging or data file compaction.
///
/// For more details, please refer to https://docs.google.com/document/d/1fIvrRfEHWBephsX0Br2G-Ils_30JIkmGkcdbFbovQjI/edit?usp=sharing
///
/// Note: this function should be called before catalog transaction commit.
///
/// # Arguments:
///
/// * data_files_to_remove: remote data file path, if non empty, both data file and deletion vector manifest entries should be updated.
/// * index_puffin_blobs_to_remove: remote file index puffin file path, if non empty, file index manifest entries should be updated.
///
/// TODO(hjiang):
/// 1. There're too many sequential IO operations to rewrite deletion vectors, need to optimize.
/// 2. Could optimize to avoid file indices manifest file to rewrite.
pub(crate) async fn append_puffin_metadata_and_rewrite(
    table_metadata: &TableMetadata,
    file_io: &FileIO,
    deletion_vector_blobs_to_add: &HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    file_index_blobs_to_add: &HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    data_files_to_remove: &HashSet<String>,
    index_puffin_blobs_to_remove: &HashSet<String>,
) -> IcebergResult<()> {
    if data_files_to_remove.is_empty()
        && deletion_vector_blobs_to_add.is_empty()
        && file_index_blobs_to_add.is_empty()
        && index_puffin_blobs_to_remove.is_empty()
    {
        return Ok(());
    }

    let cur_snapshot = table_metadata.current_snapshot().unwrap();
    let manifest_list_content = file_io
        .new_input(cur_snapshot.manifest_list())?
        .read()
        .await?;
    let manifest_list =
        ManifestList::parse_with_version(&manifest_list_content, table_metadata.format_version())?;

    // Delete existing manifest list file and rewrite.
    let mut manifest_list_writer =
        create_new_manifest_list_writer(table_metadata, cur_snapshot, file_io).await?;

    // Manifest manager for data files, deletion vectors and file indices.
    let mut data_file_manifest_manager =
        DataFileManifestManager::new(table_metadata, file_io, data_files_to_remove);
    let mut deletion_vector_manifest_manager =
        DeletionVectorManifestManager::new(table_metadata, file_io, data_files_to_remove);
    let mut file_index_manifest_manager =
        FileIndexManifestManager::new(table_metadata, file_io, index_puffin_blobs_to_remove);

    // How to tell different manifest entry types:
    // - Data file: manifest content type `Data`, manifest entry file format `Parquet`
    // - Deletion vector: manifest content type `Deletes`, manifest entry file format `Puffin`
    // - File indices: manifest content type `Data`, manifest entry file format `Puffin`
    //
    // Precondition for manifest entries updates:
    // - Data file: [`data_files_to_remove`] is non empty.
    // - Deletion vector: [`deletion_vector_blobs_to_add`] is non empty, or [`data_files_to_remove`] is non empty.
    // - File index: [`file_index_blobs_to_add`] is non empty, or [`index_puffin_blobs_to_remove`] is non empty.
    for cur_manifest_file in manifest_list.entries() {
        let manifest = cur_manifest_file.load_manifest(file_io).await?;
        let (manifest_entries, manifest_metadata) = manifest.into_parts();

        // Assumption: we store all data file manifest entries in one manifest file.
        assert!(!manifest_entries.is_empty());

        // Check for data file entries, see if there're updates.
        let manifest_entry_type =
            manifest_utils::get_manifest_entry_type(&manifest_entries, &manifest_metadata);
        if manifest_entry_type == ManifestEntryType::DataFile && data_files_to_remove.is_empty() {
            manifest_list_writer.add_manifests([cur_manifest_file.clone()].into_iter())?;
            continue;
        }

        // Check for deletion vector entries, see if there're updates.
        if manifest_entry_type == ManifestEntryType::DeletionVector
            && deletion_vector_blobs_to_add.is_empty()
            && data_files_to_remove.is_empty()
        {
            manifest_list_writer.add_manifests([cur_manifest_file.clone()].into_iter())?;
            continue;
        }

        // Check for file index entries, see if there're updates.
        if manifest_entry_type == ManifestEntryType::FileIndex
            && file_index_blobs_to_add.is_empty()
            && index_puffin_blobs_to_remove.is_empty()
        {
            manifest_list_writer.add_manifests([cur_manifest_file.clone()].into_iter())?;
            continue;
        }

        match manifest_entry_type {
            ManifestEntryType::DataFile => {
                data_file_manifest_manager
                    .add_manifest_entries(manifest_entries, manifest_metadata)
                    .await?;
            }
            ManifestEntryType::DeletionVector => {
                deletion_vector_manifest_manager
                    .add_manifest_entries(manifest_entries, manifest_metadata)?;
            }
            ManifestEntryType::FileIndex => {
                file_index_manifest_manager
                    .add_manifest_entries(manifest_entries, manifest_metadata)?;
            }
        }
    }

    // Append puffin blobs into existing manifest entries.
    deletion_vector_manifest_manager.add_new_puffin_blobs(deletion_vector_blobs_to_add)?;
    file_index_manifest_manager.add_new_puffin_blobs(file_index_blobs_to_add)?;

    // Attempt to finalize all existing manifest entries.
    if let Some(manifest_file) = data_file_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }
    if let Some(manifest_file) = deletion_vector_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }
    if let Some(manifest_file) = file_index_manifest_manager.finalize().await? {
        manifest_list_writer.add_manifests(std::iter::once(manifest_file))?;
    }

    // Flush the manifest list, there's no need to rewrite metadata.
    manifest_list_writer.close().await?;

    Ok(())
}
