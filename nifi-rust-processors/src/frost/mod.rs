//! FROST processors module

pub mod consume_frost_bus;
pub mod deploy_k8s;
pub mod lookup_build_config;
pub mod publish_frost_bus;
pub mod self_deploy_gate;
pub mod trigger_kaniko;

/// Common frost-bus job structure (mirrors frost-bus::Job)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FrostJob {
    pub id: uuid::Uuid,
    pub job_type: String,
    pub payload: serde_json::Value,
    pub session_id: Option<uuid::Uuid>,
    pub user_sub: Option<String>,
    pub workspace_id: Option<uuid::Uuid>,
    pub project_id: Option<uuid::Uuid>,
    pub status: String,
    pub created_at: i64,
}

impl FrostJob {
    pub fn new(job_type: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            job_type: job_type.into(),
            payload,
            session_id: None,
            user_sub: None,
            workspace_id: None,
            project_id: None,
            status: "queued".to_string(),
            created_at: chrono::Utc::now().timestamp_millis(),
        }
    }
}

/// Standard FlowFile attribute names for FROST processors
pub mod attributes {
    pub const FROST_JOB_ID: &str = "frost.job.id";
    pub const FROST_JOB_TYPE: &str = "frost.job.type";
    pub const FROST_WORKSPACE_ID: &str = "frost.workspace.id";
    pub const FROST_PROJECT_ID: &str = "frost.project.id";
    pub const FROST_BUILD_JOB: &str = "frost.build.job";
    pub const FROST_BUILD_IMAGE: &str = "frost.build.image";
    pub const FROST_DEPLOY_STATUS: &str = "frost.deploy.status";
}
