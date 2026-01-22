//! LookupBuildConfig Processor
//!
//! Queries Plane database to look up WorkspaceBuildConfig for a repository.
//! Used to determine build configuration when a GitHub webhook is received.

use super::attributes;
use nifi_rust_api::prelude::*;
use sqlx::postgres::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Build configuration from Plane database
#[derive(Debug, Clone, serde::Serialize)]
pub struct BuildConfig {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub git_repo: String,
    pub git_branch: String,
    pub dockerfile_path: String,
    pub context_path: String,
    pub image_name: String,
    pub build_args: serde_json::Value,
    pub status: String,
    pub auto_build_on_push: bool,
    pub auto_build_branch_pattern: String,
    pub memory_request: String,
    pub memory_limit: String,
    pub cpu_request: String,
    pub cpu_limit: String,
    pub auto_deploy: bool,
    pub deploy_target: Option<String>,
    pub deploy_namespace: String,
}

/// Processor that looks up build configuration from Plane database
pub struct LookupBuildConfig {
    /// Database connection pool
    db_pool: Option<PgPool>,
}

impl Default for LookupBuildConfig {
    fn default() -> Self {
        Self { db_pool: None }
    }
}

#[async_trait]
impl Processor for LookupBuildConfig {
    fn get_type(&self) -> &'static str {
        "org.apache.nifi.frost.LookupBuildConfig"
    }

    fn get_description(&self) -> &'static str {
        "Looks up WorkspaceBuildConfig from Plane database based on git repository URL"
    }

    fn get_property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PropertyDescriptor::new("plane.database.url")
                .display_name("Plane Database URL")
                .description("PostgreSQL connection URL for Plane database")
                .required(true)
                .sensitive(true)
                .build(),
        ]
    }

    fn get_relationships(&self) -> Vec<Relationship> {
        vec![
            Relationship::SUCCESS,
            Relationship::new("no_config")
                .description("FlowFiles sent here when no matching build config is found"),
            Relationship::FAILURE,
        ]
    }

    async fn on_scheduled(&mut self, context: &ProcessContext) -> Result<(), ProcessorError> {
        // Get database URL
        let db_url = context
            .get_property_string("plane.database.url")
            .ok_or_else(|| {
                ProcessorError::InvalidConfig("plane.database.url is required".to_string())
            })?;

        // Create connection pool
        self.db_pool = Some(
            PgPool::connect(&db_url)
                .await
                .map_err(|e| ProcessorError::Other(format!("Failed to connect to database: {}", e)))?,
        );

        info!("LookupBuildConfig scheduled - connected to Plane database");
        Ok(())
    }

    async fn on_trigger(
        &mut self,
        _context: &ProcessContext,
        session: &mut ProcessSession,
    ) -> Result<(), ProcessorError> {
        let pool = self.db_pool.as_ref().ok_or_else(|| {
            ProcessorError::NotInitialized("Database pool not initialized".to_string())
        })?;

        while let Some(mut flowfile) = session.get() {
            // Get git repo URL from payload or attribute
            let git_repo = self.extract_git_repo(&flowfile)?;

            // Get workspace ID if available
            let workspace_id = flowfile
                .get_attribute(attributes::FROST_WORKSPACE_ID)
                .and_then(|s| Uuid::parse_str(s).ok());

            // Look up build config
            match self.lookup_config(pool, &git_repo, workspace_id).await {
                Ok(Some(config)) => {
                    // Found config - add to FlowFile
                    info!(
                        config_id = %config.id,
                        config_name = %config.name,
                        git_repo = %git_repo,
                        "Found build config"
                    );

                    // Add config attributes
                    flowfile.put_attribute("frost.build.config.id", &config.id.to_string());
                    flowfile.put_attribute("frost.build.config.name", &config.name);
                    flowfile.put_attribute("frost.build.dockerfile", &config.dockerfile_path);
                    flowfile.put_attribute("frost.build.context", &config.context_path);
                    flowfile.put_attribute("frost.build.image_name", &config.image_name);
                    flowfile.put_attribute("frost.build.memory_request", &config.memory_request);
                    flowfile.put_attribute("frost.build.memory_limit", &config.memory_limit);
                    flowfile.put_attribute("frost.build.cpu_request", &config.cpu_request);
                    flowfile.put_attribute("frost.build.cpu_limit", &config.cpu_limit);

                    // Auto-deploy attributes
                    if config.auto_deploy {
                        flowfile.put_attribute("frost.deploy.enabled", "true");
                        if let Some(target) = &config.deploy_target {
                            flowfile.put_attribute("frost.deploy.target", target);
                        }
                        flowfile.put_attribute("frost.deploy.namespace", &config.deploy_namespace);
                    }

                    // Update workspace ID if not already set
                    if workspace_id.is_none() {
                        flowfile.put_attribute(
                            attributes::FROST_WORKSPACE_ID,
                            &config.workspace_id.to_string(),
                        );
                    }

                    // Put full config in content for downstream use
                    let config_json = serde_json::to_vec(&config).unwrap_or_default();
                    flowfile.set_content(bytes::Bytes::from(config_json));

                    session.transfer(flowfile, Relationship::SUCCESS);
                }
                Ok(None) => {
                    // No config found
                    warn!(
                        git_repo = %git_repo,
                        workspace_id = ?workspace_id,
                        "No build config found"
                    );
                    flowfile.put_attribute("frost.lookup.status", "not_found");
                    session.transfer(flowfile, Relationship::new("no_config"));
                }
                Err(e) => {
                    error!(
                        error = %e,
                        git_repo = %git_repo,
                        "Failed to lookup build config"
                    );
                    flowfile.put_attribute("frost.lookup.error", &e.to_string());
                    session.transfer(flowfile, Relationship::FAILURE);
                }
            }
        }

        Ok(())
    }

    async fn on_stopped(&mut self, _context: &ProcessContext) -> Result<(), ProcessorError> {
        if let Some(pool) = self.db_pool.take() {
            pool.close().await;
        }
        info!("LookupBuildConfig stopped");
        Ok(())
    }
}

impl LookupBuildConfig {
    /// Extract git repo URL from FlowFile (payload or attribute)
    fn extract_git_repo(&self, flowfile: &FlowFile) -> Result<String, ProcessorError> {
        // Try attribute first
        if let Some(repo) = flowfile.get_attribute("frost.git.repo") {
            return Ok(repo.to_string());
        }

        // Try to parse from payload
        if let Some(content) = flowfile.content() {
            let payload: serde_json::Value = serde_json::from_slice(content).map_err(|e| {
                ProcessorError::Serialization(format!("Failed to parse payload: {}", e))
            })?;

            // GitHub webhook push event has repository.clone_url or repository.html_url
            if let Some(repo_url) = payload
                .get("repository")
                .and_then(|r| r.get("clone_url").or_else(|| r.get("html_url")))
                .and_then(|v| v.as_str())
            {
                return Ok(repo_url.to_string());
            }

            // Also check repository.full_name for matching
            if let Some(full_name) = payload
                .get("repository")
                .and_then(|r| r.get("full_name"))
                .and_then(|v| v.as_str())
            {
                // Convert full_name to URL
                return Ok(format!("https://github.com/{}", full_name));
            }
        }

        Err(ProcessorError::InvalidConfig(
            "No git repository URL found in FlowFile".to_string(),
        ))
    }

    /// Look up build configuration from database
    async fn lookup_config(
        &self,
        pool: &PgPool,
        git_repo: &str,
        workspace_id: Option<Uuid>,
    ) -> Result<Option<BuildConfig>, ProcessorError> {
        // Normalize git repo URL for matching
        let normalized_repo = self.normalize_git_url(git_repo);

        // Build query based on whether we have workspace_id
        let config = if let Some(ws_id) = workspace_id {
            sqlx::query_as::<_, (
                Uuid, Uuid, String, String, String, String, String, String,
                serde_json::Value, String, bool, String, String, String, String, String,
                bool, Option<String>, String,
            )>(
                r#"
                SELECT
                    id, workspace_id, name, git_repo, git_branch, dockerfile_path,
                    context_path, image_name, build_args, status, auto_build_on_push,
                    auto_build_branch_pattern, memory_request, memory_limit, cpu_request, cpu_limit,
                    auto_deploy, deploy_target, deploy_namespace
                FROM workspace_build_configs
                WHERE workspace_id = $1
                  AND (git_repo = $2 OR git_repo LIKE $3)
                  AND status = 'active'
                  AND deleted_at IS NULL
                LIMIT 1
                "#
            )
            .bind(ws_id)
            .bind(&normalized_repo)
            .bind(format!("%{}", &normalized_repo))
            .fetch_optional(pool)
            .await
        } else {
            sqlx::query_as::<_, (
                Uuid, Uuid, String, String, String, String, String, String,
                serde_json::Value, String, bool, String, String, String, String, String,
                bool, Option<String>, String,
            )>(
                r#"
                SELECT
                    id, workspace_id, name, git_repo, git_branch, dockerfile_path,
                    context_path, image_name, build_args, status, auto_build_on_push,
                    auto_build_branch_pattern, memory_request, memory_limit, cpu_request, cpu_limit,
                    auto_deploy, deploy_target, deploy_namespace
                FROM workspace_build_configs
                WHERE (git_repo = $1 OR git_repo LIKE $2)
                  AND status = 'active'
                  AND deleted_at IS NULL
                LIMIT 1
                "#
            )
            .bind(&normalized_repo)
            .bind(format!("%{}", &normalized_repo))
            .fetch_optional(pool)
            .await
        };

        match config {
            Ok(Some(row)) => Ok(Some(BuildConfig {
                id: row.0,
                workspace_id: row.1,
                name: row.2,
                git_repo: row.3,
                git_branch: row.4,
                dockerfile_path: row.5,
                context_path: row.6,
                image_name: row.7,
                build_args: row.8,
                status: row.9,
                auto_build_on_push: row.10,
                auto_build_branch_pattern: row.11,
                memory_request: row.12,
                memory_limit: row.13,
                cpu_request: row.14,
                cpu_limit: row.15,
                auto_deploy: row.16,
                deploy_target: row.17,
                deploy_namespace: row.18,
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(ProcessorError::Other(format!("Database query failed: {}", e))),
        }
    }

    /// Normalize git URL for matching (remove .git suffix, ensure https)
    fn normalize_git_url(&self, url: &str) -> String {
        let mut normalized = url.to_string();

        // Remove trailing .git
        if normalized.ends_with(".git") {
            normalized = normalized[..normalized.len() - 4].to_string();
        }

        // Convert git@ to https://
        if normalized.starts_with("git@github.com:") {
            normalized = normalized
                .replace("git@github.com:", "https://github.com/");
        }

        normalized
    }
}

// Register the processor
nifi_rust_api::register_processor!(LookupBuildConfig);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_metadata() {
        let processor = LookupBuildConfig::default();
        assert_eq!(processor.get_type(), "org.apache.nifi.frost.LookupBuildConfig");
        assert!(!processor.get_property_descriptors().is_empty());
        assert_eq!(processor.get_relationships().len(), 3);
    }

    #[test]
    fn test_normalize_git_url() {
        let processor = LookupBuildConfig::default();

        assert_eq!(
            processor.normalize_git_url("https://github.com/owner/repo.git"),
            "https://github.com/owner/repo"
        );

        assert_eq!(
            processor.normalize_git_url("git@github.com:owner/repo.git"),
            "https://github.com/owner/repo"
        );

        assert_eq!(
            processor.normalize_git_url("https://github.com/owner/repo"),
            "https://github.com/owner/repo"
        );
    }
}
