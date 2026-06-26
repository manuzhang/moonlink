use iceberg::{TableCommit, TableIdent, TableRequirement, TableUpdate};

/// Struct which mimics [`TableCommit`], because [`TableCommit`] does not expose a public constructor.
#[repr(C)]
pub(crate) struct TableCommitProxy {
    pub(crate) ident: TableIdent,
    pub(crate) requirements: Vec<TableRequirement>,
    pub(crate) updates: Vec<TableUpdate>,
}

impl TableCommitProxy {
    /// Take as [`TableCommit`].
    pub(crate) fn take_as_table_commit(self) -> TableCommit {
        unsafe { std::mem::transmute::<TableCommitProxy, TableCommit>(self) }
    }
}
