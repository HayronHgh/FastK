use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension, Row};

use crate::error::{FastKError, Result};

/// Minimal pointer to a deterministic versioned FastK dataset root.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatasetRef {
    pub dataset_id: String,
    pub version: String,
    pub fastk_root: String,
}

impl DatasetRef {
    pub fn new(dataset_id: &str, version: &str, fastk_root: impl Into<String>) -> Result<Self> {
        validate_required("dataset_id", dataset_id)?;
        validate_required("version", version)?;
        let fastk_root = fastk_root.into();
        validate_required("fastk_root", &fastk_root)?;
        Ok(Self {
            dataset_id: dataset_id.to_string(),
            version: version.to_string(),
            fastk_root,
        })
    }

    pub fn versioned(base_root: impl AsRef<Path>, dataset_id: &str, version: &str) -> Result<Self> {
        let root = versioned_dataset_root(base_root, dataset_id, version);
        Self::new(dataset_id, version, root.to_string_lossy().into_owned())
    }

    pub fn fastk_root_path(&self) -> PathBuf {
        PathBuf::from(&self.fastk_root)
    }
}

/// Builds the recommended versioned FastK root path: `<base>/datasets/<dataset_id>/<version>`.
pub fn versioned_dataset_root(
    base_root: impl AsRef<Path>,
    dataset_id: &str,
    version: &str,
) -> PathBuf {
    base_root
        .as_ref()
        .join("datasets")
        .join(dataset_id)
        .join(version)
}

/// SQLite row backing a versioned FastK dataset.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatasetManifestRecord {
    pub dataset_id: String,
    pub version: String,
    pub fastk_root: String,
    pub source: String,
    pub market: String,
    pub start_ts: i64,
    pub end_ts: i64,
    pub schema_version: String,
    pub checksum: Option<String>,
    pub status: String,
    pub created_at: i64,
}

impl DatasetManifestRecord {
    pub fn dataset_ref(&self) -> DatasetRef {
        DatasetRef {
            dataset_id: self.dataset_id.clone(),
            version: self.version.clone(),
            fastk_root: self.fastk_root.clone(),
        }
    }

    fn validate(&self) -> Result<()> {
        validate_required("dataset_id", &self.dataset_id)?;
        validate_required("version", &self.version)?;
        validate_required("fastk_root", &self.fastk_root)?;
        validate_required("source", &self.source)?;
        validate_required("market", &self.market)?;
        validate_required("schema_version", &self.schema_version)?;
        validate_required("status", &self.status)?;
        if self.start_ts > self.end_ts {
            return Err(FastKError::InvalidInput(format!(
                "dataset {}@{} has start_ts {} after end_ts {}",
                self.dataset_id, self.version, self.start_ts, self.end_ts
            )));
        }
        Ok(())
    }
}

/// Registry row for scalar feature outputs stored in FastK.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FeatureRegistryRecord {
    pub feature_id: String,
    pub name: String,
    pub version: String,
    pub input_dataset_id: String,
    pub input_dataset_version: String,
    pub fastk_series_key: String,
    pub config_hash: String,
    pub code_version: Option<String>,
    pub created_at: i64,
    pub status: String,
}

impl FeatureRegistryRecord {
    fn validate(&self) -> Result<()> {
        validate_required("feature_id", &self.feature_id)?;
        validate_required("name", &self.name)?;
        validate_required("version", &self.version)?;
        validate_required("input_dataset_id", &self.input_dataset_id)?;
        validate_required("input_dataset_version", &self.input_dataset_version)?;
        validate_required("fastk_series_key", &self.fastk_series_key)?;
        validate_required("config_hash", &self.config_hash)?;
        validate_required("status", &self.status)?;
        Ok(())
    }
}

/// Registry row for scalar factor outputs stored in FastK.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FactorRegistryRecord {
    pub factor_id: String,
    pub name: String,
    pub version: String,
    pub input_dataset_id: String,
    pub input_dataset_version: String,
    pub input_feature_refs: String,
    pub fastk_series_key: String,
    pub formula: Option<String>,
    pub method: Option<String>,
    pub config_hash: String,
    pub code_version: Option<String>,
    pub created_at: i64,
    pub status: String,
}

impl FactorRegistryRecord {
    fn validate(&self) -> Result<()> {
        validate_required("factor_id", &self.factor_id)?;
        validate_required("name", &self.name)?;
        validate_required("version", &self.version)?;
        validate_required("input_dataset_id", &self.input_dataset_id)?;
        validate_required("input_dataset_version", &self.input_dataset_version)?;
        validate_required("input_feature_refs", &self.input_feature_refs)?;
        validate_required("fastk_series_key", &self.fastk_series_key)?;
        validate_required("config_hash", &self.config_hash)?;
        validate_required("status", &self.status)?;
        Ok(())
    }
}

/// SQLite-backed control catalog for dataset, feature, and factor metadata.
///
/// FastK remains the binary time-series data plane. This catalog stores relational control-plane
/// metadata that should not be embedded into every chunk.
pub struct DatasetRegistry {
    conn: Connection,
}

impl DatasetRegistry {
    /// Opens or creates a registry database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let registry = Self {
            conn: Connection::open(path)?,
        };
        registry.init()?;
        Ok(registry)
    }

    /// Creates an in-memory registry, primarily for tests and short-lived tools.
    pub fn in_memory() -> Result<Self> {
        let registry = Self {
            conn: Connection::open_in_memory()?,
        };
        registry.init()?;
        Ok(registry)
    }

    /// Ensures all control-plane tables exist and performs additive compatibility migrations.
    pub fn init(&self) -> Result<()> {
        self.conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        self.ensure_dataset_manifest_schema()?;
        self.ensure_feature_registry_schema()?;
        self.ensure_factor_registry_schema()?;
        Ok(())
    }

    /// Inserts or updates a dataset manifest row for one explicit `(dataset_id, version)` pair.
    pub fn upsert_dataset(&self, record: &DatasetManifestRecord) -> Result<()> {
        record.validate()?;
        self.conn.execute(
            r#"
            INSERT INTO dataset_manifest (
                dataset_id, version, fastk_root, source, market, start_ts, end_ts,
                schema_version, checksum, status, created_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(dataset_id, version) DO UPDATE SET
                fastk_root = excluded.fastk_root,
                source = excluded.source,
                market = excluded.market,
                start_ts = excluded.start_ts,
                end_ts = excluded.end_ts,
                schema_version = excluded.schema_version,
                checksum = excluded.checksum,
                status = excluded.status,
                created_at = excluded.created_at
            "#,
            params![
                &record.dataset_id,
                &record.version,
                &record.fastk_root,
                &record.source,
                &record.market,
                record.start_ts,
                record.end_ts,
                &record.schema_version,
                &record.checksum,
                &record.status,
                record.created_at,
            ],
        )?;
        Ok(())
    }

    /// Loads one deterministic dataset manifest row.
    pub fn get_dataset(
        &self,
        dataset_id: &str,
        version: &str,
    ) -> Result<Option<DatasetManifestRecord>> {
        validate_required("dataset_id", dataset_id)?;
        validate_required("version", version)?;
        self.conn
            .query_row(
                DATASET_SELECT_SQL_WITH_WHERE,
                params![dataset_id, version],
                dataset_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Loads the latest row by `created_at` for interactive tools.
    ///
    /// Deterministic backtests should use [`Self::get_dataset`] or [`Self::dataset_ref`] with an
    /// explicit version instead of this latest resolver.
    pub fn get_latest_dataset(&self, dataset_id: &str) -> Result<Option<DatasetManifestRecord>> {
        validate_required("dataset_id", dataset_id)?;
        self.conn
            .query_row(
                r#"
                SELECT dataset_id, version, fastk_root, source, market, start_ts, end_ts,
                       schema_version, checksum, status, created_at
                FROM dataset_manifest
                WHERE dataset_id = ?1
                ORDER BY created_at DESC, version DESC
                LIMIT 1
                "#,
                params![dataset_id],
                dataset_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Lists all dataset manifest rows ordered by deterministic key.
    pub fn list_datasets(&self) -> Result<Vec<DatasetManifestRecord>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT dataset_id, version, fastk_root, source, market, start_ts, end_ts,
                   schema_version, checksum, status, created_at
            FROM dataset_manifest
            ORDER BY dataset_id, version
            "#,
        )?;
        collect_rows(&mut stmt, dataset_from_row)
    }

    /// Returns a compact dataset reference by explicit id and version, if it exists.
    pub fn dataset_ref(&self, dataset_id: &str, version: &str) -> Result<Option<DatasetRef>> {
        Ok(self
            .get_dataset(dataset_id, version)?
            .map(|record| record.dataset_ref()))
    }

    /// Resolves the FastK root for an explicit dataset id and version.
    pub fn resolve_fastk_root(&self, dataset_id: &str, version: &str) -> Result<PathBuf> {
        self.dataset_ref(dataset_id, version)?
            .map(|dataset| dataset.fastk_root_path())
            .ok_or_else(|| {
                FastKError::NotFound(format!("dataset not found: {dataset_id}@{version}"))
            })
    }

    /// Resolves an explicit [`DatasetRef`] after verifying the registry has the same binding.
    pub fn resolve_dataset_ref(&self, dataset_ref: &DatasetRef) -> Result<PathBuf> {
        let stored = self
            .dataset_ref(&dataset_ref.dataset_id, &dataset_ref.version)?
            .ok_or_else(|| {
                FastKError::NotFound(format!(
                    "dataset not found: {}@{}",
                    dataset_ref.dataset_id, dataset_ref.version
                ))
            })?;
        if stored.fastk_root != dataset_ref.fastk_root {
            return Err(FastKError::InvalidInput(format!(
                "dataset ref root mismatch for {}@{}: registry={} ref={}",
                dataset_ref.dataset_id,
                dataset_ref.version,
                stored.fastk_root,
                dataset_ref.fastk_root
            )));
        }
        Ok(stored.fastk_root_path())
    }

    /// Resolves the latest FastK root by `created_at` for CLI/interactive use only.
    pub fn resolve_latest_fastk_root(&self, dataset_id: &str) -> Result<PathBuf> {
        self.get_latest_dataset(dataset_id)?
            .map(|dataset| dataset.dataset_ref().fastk_root_path())
            .ok_or_else(|| FastKError::NotFound(format!("dataset not found: {dataset_id}")))
    }

    /// Inserts or updates a feature registry row.
    pub fn upsert_feature(&self, record: &FeatureRegistryRecord) -> Result<()> {
        record.validate()?;
        self.conn.execute(
            r#"
            INSERT INTO feature_registry (
                feature_id, name, version, input_dataset_id, input_dataset_version,
                fastk_series_key, config_hash, code_version, created_at, status
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(feature_id) DO UPDATE SET
                name = excluded.name,
                version = excluded.version,
                input_dataset_id = excluded.input_dataset_id,
                input_dataset_version = excluded.input_dataset_version,
                fastk_series_key = excluded.fastk_series_key,
                config_hash = excluded.config_hash,
                code_version = excluded.code_version,
                created_at = excluded.created_at,
                status = excluded.status
            "#,
            params![
                &record.feature_id,
                &record.name,
                &record.version,
                &record.input_dataset_id,
                &record.input_dataset_version,
                &record.fastk_series_key,
                &record.config_hash,
                &record.code_version,
                record.created_at,
                &record.status,
            ],
        )?;
        Ok(())
    }

    /// Loads one feature registry row by id.
    pub fn get_feature(&self, feature_id: &str) -> Result<Option<FeatureRegistryRecord>> {
        validate_required("feature_id", feature_id)?;
        self.conn
            .query_row(
                r#"
                SELECT feature_id, name, version, input_dataset_id, input_dataset_version,
                       fastk_series_key, config_hash, code_version, created_at, status
                FROM feature_registry
                WHERE feature_id = ?1
                "#,
                params![feature_id],
                feature_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Lists all features, optionally constrained by input dataset id.
    pub fn list_features(
        &self,
        input_dataset_id: Option<&str>,
    ) -> Result<Vec<FeatureRegistryRecord>> {
        match input_dataset_id {
            Some(input_dataset_id) => {
                validate_required("input_dataset_id", input_dataset_id)?;
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT feature_id, name, version, input_dataset_id, input_dataset_version,
                           fastk_series_key, config_hash, code_version, created_at, status
                    FROM feature_registry
                    WHERE input_dataset_id = ?1
                    ORDER BY feature_id
                    "#,
                )?;
                collect_rows_with_param(&mut stmt, params![input_dataset_id], feature_from_row)
            }
            None => {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT feature_id, name, version, input_dataset_id, input_dataset_version,
                           fastk_series_key, config_hash, code_version, created_at, status
                    FROM feature_registry
                    ORDER BY feature_id
                    "#,
                )?;
                collect_rows(&mut stmt, feature_from_row)
            }
        }
    }

    /// Lists features constrained by explicit input dataset id and version.
    pub fn list_features_for_dataset(
        &self,
        input_dataset_id: &str,
        input_dataset_version: &str,
    ) -> Result<Vec<FeatureRegistryRecord>> {
        validate_required("input_dataset_id", input_dataset_id)?;
        validate_required("input_dataset_version", input_dataset_version)?;
        let mut stmt = self.conn.prepare(
            r#"
            SELECT feature_id, name, version, input_dataset_id, input_dataset_version,
                   fastk_series_key, config_hash, code_version, created_at, status
            FROM feature_registry
            WHERE input_dataset_id = ?1 AND input_dataset_version = ?2
            ORDER BY feature_id
            "#,
        )?;
        collect_rows_with_param(
            &mut stmt,
            params![input_dataset_id, input_dataset_version],
            feature_from_row,
        )
    }

    /// Inserts or updates a factor registry row.
    pub fn upsert_factor(&self, record: &FactorRegistryRecord) -> Result<()> {
        record.validate()?;
        self.conn.execute(
            r#"
            INSERT INTO factor_registry (
                factor_id, name, version, input_dataset_id, input_dataset_version,
                input_feature_refs, fastk_series_key, formula, method, config_hash,
                code_version, created_at, status
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(factor_id) DO UPDATE SET
                name = excluded.name,
                version = excluded.version,
                input_dataset_id = excluded.input_dataset_id,
                input_dataset_version = excluded.input_dataset_version,
                input_feature_refs = excluded.input_feature_refs,
                fastk_series_key = excluded.fastk_series_key,
                formula = excluded.formula,
                method = excluded.method,
                config_hash = excluded.config_hash,
                code_version = excluded.code_version,
                created_at = excluded.created_at,
                status = excluded.status
            "#,
            params![
                &record.factor_id,
                &record.name,
                &record.version,
                &record.input_dataset_id,
                &record.input_dataset_version,
                &record.input_feature_refs,
                &record.fastk_series_key,
                &record.formula,
                &record.method,
                &record.config_hash,
                &record.code_version,
                record.created_at,
                &record.status,
            ],
        )?;
        Ok(())
    }

    /// Loads one factor registry row by id.
    pub fn get_factor(&self, factor_id: &str) -> Result<Option<FactorRegistryRecord>> {
        validate_required("factor_id", factor_id)?;
        self.conn
            .query_row(
                r#"
                SELECT factor_id, name, version, input_dataset_id, input_dataset_version,
                       input_feature_refs, fastk_series_key, formula, method, config_hash,
                       code_version, created_at, status
                FROM factor_registry
                WHERE factor_id = ?1
                "#,
                params![factor_id],
                factor_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Lists all factors, optionally constrained by input dataset id.
    pub fn list_factors(
        &self,
        input_dataset_id: Option<&str>,
    ) -> Result<Vec<FactorRegistryRecord>> {
        match input_dataset_id {
            Some(input_dataset_id) => {
                validate_required("input_dataset_id", input_dataset_id)?;
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT factor_id, name, version, input_dataset_id, input_dataset_version,
                           input_feature_refs, fastk_series_key, formula, method, config_hash,
                           code_version, created_at, status
                    FROM factor_registry
                    WHERE input_dataset_id = ?1
                    ORDER BY factor_id
                    "#,
                )?;
                collect_rows_with_param(&mut stmt, params![input_dataset_id], factor_from_row)
            }
            None => {
                let mut stmt = self.conn.prepare(
                    r#"
                    SELECT factor_id, name, version, input_dataset_id, input_dataset_version,
                           input_feature_refs, fastk_series_key, formula, method, config_hash,
                           code_version, created_at, status
                    FROM factor_registry
                    ORDER BY factor_id
                    "#,
                )?;
                collect_rows(&mut stmt, factor_from_row)
            }
        }
    }

    /// Lists factors constrained by explicit input dataset id and version.
    pub fn list_factors_for_dataset(
        &self,
        input_dataset_id: &str,
        input_dataset_version: &str,
    ) -> Result<Vec<FactorRegistryRecord>> {
        validate_required("input_dataset_id", input_dataset_id)?;
        validate_required("input_dataset_version", input_dataset_version)?;
        let mut stmt = self.conn.prepare(
            r#"
            SELECT factor_id, name, version, input_dataset_id, input_dataset_version,
                   input_feature_refs, fastk_series_key, formula, method, config_hash,
                   code_version, created_at, status
            FROM factor_registry
            WHERE input_dataset_id = ?1 AND input_dataset_version = ?2
            ORDER BY factor_id
            "#,
        )?;
        collect_rows_with_param(
            &mut stmt,
            params![input_dataset_id, input_dataset_version],
            factor_from_row,
        )
    }

    fn ensure_dataset_manifest_schema(&self) -> Result<()> {
        if !table_exists(&self.conn, "dataset_manifest")? {
            self.create_dataset_manifest_table()?;
            return Ok(());
        }

        let pk = primary_key_columns(&self.conn, "dataset_manifest")?;
        if pk != ["dataset_id".to_string(), "version".to_string()] {
            self.conn.execute_batch(
                r#"
                ALTER TABLE dataset_manifest RENAME TO dataset_manifest_legacy_v1;
                "#,
            )?;
            self.create_dataset_manifest_table()?;
            self.conn.execute_batch(
                r#"
                INSERT OR REPLACE INTO dataset_manifest (
                    dataset_id, version, fastk_root, source, market, start_ts, end_ts,
                    schema_version, checksum, status, created_at
                )
                SELECT dataset_id, version, fastk_root, source, market, start_ts, end_ts,
                       schema_version, checksum, status, created_at
                FROM dataset_manifest_legacy_v1;
                DROP TABLE dataset_manifest_legacy_v1;
                "#,
            )?;
        }
        self.conn.execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_dataset_manifest_status
                ON dataset_manifest(status);
            CREATE INDEX IF NOT EXISTS idx_dataset_manifest_latest
                ON dataset_manifest(dataset_id, created_at DESC, version DESC);
            "#,
        )?;
        Ok(())
    }

    fn create_dataset_manifest_table(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS dataset_manifest (
                dataset_id TEXT NOT NULL,
                version TEXT NOT NULL,
                fastk_root TEXT NOT NULL,
                source TEXT NOT NULL,
                market TEXT NOT NULL,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL,
                schema_version TEXT NOT NULL,
                checksum TEXT,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (dataset_id, version)
            );

            CREATE INDEX IF NOT EXISTS idx_dataset_manifest_status
                ON dataset_manifest(status);
            CREATE INDEX IF NOT EXISTS idx_dataset_manifest_latest
                ON dataset_manifest(dataset_id, created_at DESC, version DESC);
            "#,
        )?;
        Ok(())
    }

    fn ensure_feature_registry_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS feature_registry (
                feature_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                version TEXT NOT NULL,
                input_dataset_id TEXT NOT NULL,
                input_dataset_version TEXT NOT NULL DEFAULT '',
                fastk_series_key TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                code_version TEXT,
                created_at INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active'
            );
            "#,
        )?;
        add_text_column_if_missing(
            &self.conn,
            "feature_registry",
            "input_dataset_version",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        add_text_column_if_missing(
            &self.conn,
            "feature_registry",
            "status",
            "TEXT NOT NULL DEFAULT 'active'",
        )?;
        self.conn.execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_feature_registry_input_dataset
                ON feature_registry(input_dataset_id, input_dataset_version);
            "#,
        )?;
        Ok(())
    }

    fn ensure_factor_registry_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS factor_registry (
                factor_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                version TEXT NOT NULL,
                input_dataset_id TEXT NOT NULL,
                input_dataset_version TEXT NOT NULL DEFAULT '',
                input_feature_refs TEXT NOT NULL DEFAULT '[]',
                fastk_series_key TEXT NOT NULL,
                formula TEXT,
                method TEXT,
                config_hash TEXT NOT NULL,
                code_version TEXT,
                created_at INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active'
            );
            "#,
        )?;
        add_text_column_if_missing(
            &self.conn,
            "factor_registry",
            "input_dataset_id",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        add_text_column_if_missing(
            &self.conn,
            "factor_registry",
            "input_dataset_version",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        add_text_column_if_missing(
            &self.conn,
            "factor_registry",
            "input_feature_refs",
            "TEXT NOT NULL DEFAULT '[]'",
        )?;
        add_text_column_if_missing(&self.conn, "factor_registry", "method", "TEXT")?;
        add_text_column_if_missing(&self.conn, "factor_registry", "code_version", "TEXT")?;
        add_text_column_if_missing(
            &self.conn,
            "factor_registry",
            "status",
            "TEXT NOT NULL DEFAULT 'active'",
        )?;
        self.conn.execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_factor_registry_input_dataset
                ON factor_registry(input_dataset_id, input_dataset_version);
            "#,
        )?;
        Ok(())
    }
}

const DATASET_SELECT_SQL_WITH_WHERE: &str = r#"
SELECT dataset_id, version, fastk_root, source, market, start_ts, end_ts,
       schema_version, checksum, status, created_at
FROM dataset_manifest
WHERE dataset_id = ?1 AND version = ?2
"#;

fn collect_rows<T>(
    stmt: &mut rusqlite::Statement<'_>,
    map: fn(&Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>> {
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(map(row)?);
    }
    Ok(out)
}

fn collect_rows_with_param<T, P>(
    stmt: &mut rusqlite::Statement<'_>,
    params: P,
    map: fn(&Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>>
where
    P: rusqlite::Params,
{
    let mut rows = stmt.query(params)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(map(row)?);
    }
    Ok(out)
}

fn dataset_from_row(row: &Row<'_>) -> rusqlite::Result<DatasetManifestRecord> {
    Ok(DatasetManifestRecord {
        dataset_id: row.get(0)?,
        version: row.get(1)?,
        fastk_root: row.get(2)?,
        source: row.get(3)?,
        market: row.get(4)?,
        start_ts: row.get(5)?,
        end_ts: row.get(6)?,
        schema_version: row.get(7)?,
        checksum: row.get(8)?,
        status: row.get(9)?,
        created_at: row.get(10)?,
    })
}

fn feature_from_row(row: &Row<'_>) -> rusqlite::Result<FeatureRegistryRecord> {
    Ok(FeatureRegistryRecord {
        feature_id: row.get(0)?,
        name: row.get(1)?,
        version: row.get(2)?,
        input_dataset_id: row.get(3)?,
        input_dataset_version: row.get(4)?,
        fastk_series_key: row.get(5)?,
        config_hash: row.get(6)?,
        code_version: row.get(7)?,
        created_at: row.get(8)?,
        status: row.get(9)?,
    })
}

fn factor_from_row(row: &Row<'_>) -> rusqlite::Result<FactorRegistryRecord> {
    Ok(FactorRegistryRecord {
        factor_id: row.get(0)?,
        name: row.get(1)?,
        version: row.get(2)?,
        input_dataset_id: row.get(3)?,
        input_dataset_version: row.get(4)?,
        input_feature_refs: row.get(5)?,
        fastk_series_key: row.get(6)?,
        formula: row.get(7)?,
        method: row.get(8)?,
        config_hash: row.get(9)?,
        code_version: row.get(10)?,
        created_at: row.get(11)?,
        status: row.get(12)?,
    })
}

fn validate_required(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(FastKError::InvalidInput(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
        params![table],
        |row| row.get::<_, i64>(0),
    )
    .map(|value| value != 0)
    .map_err(Into::into)
}

fn primary_key_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = stmt.query([])?;
    let mut columns = Vec::<(i64, String)>::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        let pk: i64 = row.get(5)?;
        if pk > 0 {
            columns.push((pk, name));
        }
    }
    columns.sort_by_key(|(pk, _)| *pk);
    Ok(columns.into_iter().map(|(_, name)| name).collect())
}

fn add_text_column_if_missing(
    conn: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<()> {
    if table_columns(conn, table)?
        .iter()
        .any(|existing| existing == column)
    {
        return Ok(());
    }
    conn.execute_batch(&format!(
        "ALTER TABLE {table} ADD COLUMN {column} {definition};"
    ))?;
    Ok(())
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = stmt.query([])?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next()? {
        columns.push(row.get(1)?);
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::{
        versioned_dataset_root, DatasetManifestRecord, DatasetRef, DatasetRegistry,
        FactorRegistryRecord, FeatureRegistryRecord,
    };

    #[test]
    fn dataset_manifest_resolves_explicit_versions_without_latest_ambiguity() {
        let registry = DatasetRegistry::in_memory().expect("registry should open");
        let v1 = dataset_record(
            "v20260424",
            "data/fastk/datasets/binance_spot_clean/v20260424",
            3,
        );
        let v2 = dataset_record(
            "v20260425",
            "data/fastk/datasets/binance_spot_clean/v20260425",
            4,
        );

        registry
            .upsert_dataset(&v1)
            .expect("v1 dataset should upsert");
        registry
            .upsert_dataset(&v2)
            .expect("v2 dataset should upsert");

        assert_eq!(
            registry
                .resolve_fastk_root("binance_spot_clean", "v20260424")
                .expect("v1 root should resolve")
                .to_string_lossy()
                .replace('\\', "/"),
            "data/fastk/datasets/binance_spot_clean/v20260424"
        );
        assert_eq!(
            registry
                .resolve_fastk_root("binance_spot_clean", "v20260425")
                .expect("v2 root should resolve")
                .to_string_lossy()
                .replace('\\', "/"),
            "data/fastk/datasets/binance_spot_clean/v20260425"
        );
        assert_eq!(
            registry
                .resolve_latest_fastk_root("binance_spot_clean")
                .expect("latest root should resolve")
                .to_string_lossy()
                .replace('\\', "/"),
            "data/fastk/datasets/binance_spot_clean/v20260425"
        );
    }

    #[test]
    fn dataset_ref_resolution_checks_id_version_and_root() {
        let registry = DatasetRegistry::in_memory().expect("registry should open");
        let record = dataset_record(
            "v20260424",
            "data/fastk/datasets/binance_spot_clean/v20260424",
            3,
        );
        registry
            .upsert_dataset(&record)
            .expect("dataset should upsert");

        let dataset_ref = record.dataset_ref();
        assert_eq!(
            registry
                .resolve_dataset_ref(&dataset_ref)
                .expect("dataset ref should resolve")
                .to_string_lossy()
                .replace('\\', "/"),
            "data/fastk/datasets/binance_spot_clean/v20260424"
        );

        let mismatch = DatasetRef::new("binance_spot_clean", "v20260424", "other/root")
            .expect("dataset ref should build");
        let err = registry
            .resolve_dataset_ref(&mismatch)
            .expect_err("root mismatch should fail");
        assert!(matches!(err, crate::FastKError::InvalidInput(_)));
    }

    #[test]
    fn feature_and_factor_registry_track_dataset_lineage() {
        let registry = DatasetRegistry::in_memory().expect("registry should open");

        let feature = FeatureRegistryRecord {
            feature_id: "feature:rsi_14:v1".to_string(),
            name: "rsi_14".to_string(),
            version: "v1".to_string(),
            input_dataset_id: "binance_spot_clean".to_string(),
            input_dataset_version: "v20260424".to_string(),
            fastk_series_key: "BTCUSDT/feature/1m@@rsi_14".to_string(),
            config_hash: "cfg-feature".to_string(),
            code_version: Some("git:abc".to_string()),
            created_at: 10,
            status: "active".to_string(),
        };
        let factor = FactorRegistryRecord {
            factor_id: "factor:momentum_score:v1".to_string(),
            name: "momentum_score".to_string(),
            version: "v1".to_string(),
            input_dataset_id: "binance_spot_clean".to_string(),
            input_dataset_version: "v20260424".to_string(),
            input_feature_refs: r#"["feature:rsi_14:v1"]"#.to_string(),
            fastk_series_key: "BTCUSDT/factor/1m@@momentum_score".to_string(),
            formula: Some("zscore(momentum_20) + zscore(rsi_14)".to_string()),
            method: None,
            config_hash: "cfg-factor".to_string(),
            code_version: Some("git:def".to_string()),
            created_at: 11,
            status: "active".to_string(),
        };

        registry
            .upsert_feature(&feature)
            .expect("feature should upsert");
        registry
            .upsert_factor(&factor)
            .expect("factor should upsert");

        let loaded_feature = registry
            .get_feature("feature:rsi_14:v1")
            .expect("feature should load")
            .expect("feature should exist");
        assert_eq!(loaded_feature.input_dataset_id, "binance_spot_clean");
        assert_eq!(loaded_feature.input_dataset_version, "v20260424");
        assert_eq!(loaded_feature.config_hash, "cfg-feature");
        assert_eq!(
            loaded_feature.fastk_series_key,
            "BTCUSDT/feature/1m@@rsi_14"
        );

        let loaded_factor = registry
            .get_factor("factor:momentum_score:v1")
            .expect("factor should load")
            .expect("factor should exist");
        assert_eq!(loaded_factor.input_dataset_version, "v20260424");
        assert_eq!(loaded_factor.input_feature_refs, r#"["feature:rsi_14:v1"]"#);
        assert_eq!(
            loaded_factor.formula.as_deref(),
            Some("zscore(momentum_20) + zscore(rsi_14)")
        );
        assert_eq!(loaded_factor.config_hash, "cfg-factor");
        assert_eq!(
            registry
                .list_features_for_dataset("binance_spot_clean", "v20260424")
                .expect("features should list"),
            vec![feature]
        );
        assert_eq!(
            registry
                .list_factors_for_dataset("binance_spot_clean", "v20260424")
                .expect("factors should list"),
            vec![factor]
        );
    }

    #[test]
    fn versioned_dataset_ref_uses_stable_path_layout() {
        let root = versioned_dataset_root("data/fastk", "binance_spot_clean", "v20260424");
        assert_eq!(
            root.to_string_lossy().replace('\\', "/"),
            "data/fastk/datasets/binance_spot_clean/v20260424"
        );

        let dataset_ref = DatasetRef::versioned("data/fastk", "binance_spot_clean", "v20260424")
            .expect("dataset ref should build");
        assert_eq!(dataset_ref.dataset_id, "binance_spot_clean");
        assert_eq!(dataset_ref.version, "v20260424");
        assert_eq!(
            dataset_ref.fastk_root.replace('\\', "/"),
            "data/fastk/datasets/binance_spot_clean/v20260424"
        );
    }

    fn dataset_record(version: &str, fastk_root: &str, created_at: i64) -> DatasetManifestRecord {
        DatasetManifestRecord {
            dataset_id: "binance_spot_clean".to_string(),
            version: version.to_string(),
            fastk_root: fastk_root.to_string(),
            source: "binance".to_string(),
            market: "spot".to_string(),
            start_ts: 1,
            end_ts: 2,
            schema_version: "v1".to_string(),
            checksum: Some("abc123".to_string()),
            status: "sealed".to_string(),
            created_at,
        }
    }
}
