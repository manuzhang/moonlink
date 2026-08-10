/// A customized opendal layer, which provides chaos features like injected delay, intended errors, etc.
use std::sync::Arc;

use opendal::raw::{oio, *};
use opendal::{BytesRange, Capability, Metadata, OperationContext, Result};

use crate::storage::filesystem::accessor::chaos_generator::ChaosGenerator;
use crate::storage::filesystem::accessor_config::ChaosConfig;

/// A wrapper that delegates all operations to an inner [`FileSystemAccessor`].
#[derive(Debug)]
pub struct ChaosLayer {
    /// Chaos generator.
    chaos_generator: ChaosGenerator,
}

impl ChaosLayer {
    pub fn new(config: ChaosConfig) -> Self {
        Self {
            chaos_generator: ChaosGenerator::new(config),
        }
    }
}

impl Layer for ChaosLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(ChaosAccessor {
            chaos_generator: self.chaos_generator.clone(),
            inner,
        })
    }
}

#[derive(Debug)]
pub struct ChaosAccessor {
    /// Chaos generator.
    chaos_generator: ChaosGenerator,
    /// Inner accessor.
    inner: Servicer,
}

impl Service for ChaosAccessor {
    type Reader = ChaosReader<oio::Reader>;
    type Writer = ChaosWriter<oio::Writer>;
    type Lister = ChaosLister<oio::Lister>;
    type Deleter = ChaosDeleter<oio::Deleter>;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner
            .read(ctx, path, args)
            .map(|r| ChaosReader::new(r, self.chaos_generator.clone()))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner
            .write(ctx, path, args)
            .map(|w| ChaosWriter::new(w, self.chaos_generator.clone()))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner
            .delete(ctx)
            .map(|d| ChaosDeleter::new(d, self.chaos_generator.clone()))
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner
            .list(ctx, path, args)
            .map(|l| ChaosLister::new(l, self.chaos_generator.clone()))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args, opts)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

/// ==========================
/// Chaos reader
/// ==========================
///
pub struct ChaosReader<R> {
    /// Chaos generator.
    chaos_generator: ChaosGenerator,
    /// Inner reader.
    inner: R,
}

/// ==========================
/// Chaos lister
/// ==========================
pub struct ChaosLister<L> {
    chaos_generator: ChaosGenerator,
    inner: L,
}

impl<L> ChaosLister<L> {
    fn new(inner: L, chaos_generator: ChaosGenerator) -> Self {
        Self {
            chaos_generator,
            inner,
        }
    }
}

impl<L: oio::List> oio::List for ChaosLister<L> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.next().await
    }
}

/// ==========================
/// Chaos deleter
/// ==========================
pub struct ChaosDeleter<D> {
    chaos_generator: ChaosGenerator,
    inner: D,
}

impl<D> ChaosDeleter<D> {
    fn new(inner: D, chaos_generator: ChaosGenerator) -> Self {
        Self {
            chaos_generator,
            inner,
        }
    }
}

impl<D: oio::Delete> oio::Delete for ChaosDeleter<D> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.close().await
    }
}

impl<R> ChaosReader<R> {
    fn new(inner: R, chaos_generator: ChaosGenerator) -> Self {
        Self {
            chaos_generator,
            inner,
        }
    }
}

impl<R: opendal::raw::oio::Read> opendal::raw::oio::Read for ChaosReader<R> {
    async fn open(
        &self,
        range: BytesRange,
    ) -> Result<(RpRead, Box<dyn opendal::raw::oio::ReadStreamDyn>)> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.open(range).await
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, opendal::Buffer)> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.read(range).await
    }
}

/// ==========================
/// Chaos writer
/// ==========================
///
pub struct ChaosWriter<W> {
    /// Chaos generator.
    chaos_generator: ChaosGenerator,
    /// Inner writer.
    inner: W,
}

impl<W> ChaosWriter<W> {
    fn new(inner: W, chaos_generator: ChaosGenerator) -> Self {
        Self {
            chaos_generator,
            inner,
        }
    }
}

impl<W: opendal::raw::oio::Write> opendal::raw::oio::Write for ChaosWriter<W> {
    async fn write(&mut self, bs: opendal::Buffer) -> Result<()> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.write(bs).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.abort().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.chaos_generator.perform_wrapper_function().await?;
        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
    use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
    use crate::storage::filesystem::accessor_config::AccessorConfig;
    use crate::storage::filesystem::accessor_config::RetryConfig;
    use crate::storage::filesystem::accessor_config::TimeoutConfig;
    use crate::storage::filesystem::storage_config::StorageConfig;
    use tempfile::{tempdir, TempDir};

    /// Test util function to create a filesystem accessor, based on the given chaos option.
    fn create_filesystem_accessor(
        temp_dir: &TempDir,
        chaos_config: ChaosConfig,
        timeout_config: TimeoutConfig,
    ) -> FileSystemAccessor {
        let storage_config = StorageConfig::FileSystem {
            root_directory: temp_dir.path().to_str().unwrap().to_string(),
            atomic_write_dir: None,
        };
        let accessor_config = AccessorConfig {
            storage_config,
            chaos_config: Some(chaos_config),
            retry_config: RetryConfig::default(),
            throttle_config: None,
            timeout_config,
        };
        FileSystemAccessor::new(accessor_config)
    }

    /// Test util function to write and read an object, which should succeed whether delay injected.
    async fn perform_read_write_op(filesystem_accessor: &FileSystemAccessor) {
        // Write object.
        let filename = "test_object.txt".to_string();
        let content = b"helloworld".to_vec();
        filesystem_accessor
            .write_object(&filename, content.clone())
            .await
            .unwrap();

        // Read object.
        let read_content = filesystem_accessor.read_object(&filename).await.unwrap();
        assert_eq!(read_content, content);
    }

    #[tokio::test]
    async fn test_no_delay_no_error() {
        let temp_dir = tempdir().unwrap();
        let chaos_config = ChaosConfig {
            random_seed: None,
            min_latency: std::time::Duration::ZERO,
            max_latency: std::time::Duration::ZERO,
            err_prob: 0,
        };
        let filesystem_accessor =
            create_filesystem_accessor(&temp_dir, chaos_config, TimeoutConfig::default());
        perform_read_write_op(&filesystem_accessor).await;
    }

    #[tokio::test]
    async fn test_delay_injected() {
        let temp_dir = tempdir().unwrap();
        let chaos_config = ChaosConfig {
            random_seed: None,
            min_latency: std::time::Duration::from_millis(5000),
            max_latency: std::time::Duration::from_millis(5000),
            err_prob: 0,
        };
        let timeout_config = TimeoutConfig {
            timeout: std::time::Duration::from_millis(500),
        };
        // Timeout is less than injected delay.
        let filesystem_accessor =
            create_filesystem_accessor(&temp_dir, chaos_config, timeout_config);
        let res = filesystem_accessor.read_object("FAKE_FILEPATH").await;
        assert!(res.is_err());
    }
}
