//! Admin action audit: `queue_admin_actions` rows and their queries.

use chrono::{DateTime, Utc};
use diesel::sql_types;
use diesel_async::{AsyncPgConnection, RunQueryDsl};

use crate::error::SchedulerError;
use crate::ids::{QueueName, TaskId, TaskName};
use crate::models::{ParseEnumError, TaskView, text_enum};

/// What an operator did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminActionKind {
    Replay,
    Cancel,
    Pause,
    Resume,
}

text_enum!(AdminActionKind, "admin action", {
    Replay => "replay", Cancel => "cancel", Pause => "pause", Resume => "resume",
});

/// One `queue_admin_actions` row.
#[derive(Debug, Clone)]
pub struct AdminAction {
    pub id: i64,
    pub action: AdminActionKind,
    pub task_id: Option<TaskId>,
    pub task_name: Option<TaskName>,
    pub queue: Option<QueueName>,
    pub last_error_filter: Option<String>,
    /// Bulk id bound: the action covered only tasks with `id <=` this.
    pub max_task_id: Option<i64>,
    pub affected: i64,
    pub operator: String,
    pub reason: String,
    pub at: DateTime<Utc>,
}

#[derive(diesel::QueryableByName)]
struct ActionRow {
    #[diesel(sql_type = sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = sql_types::Text)]
    action: String,
    #[diesel(sql_type = sql_types::Nullable<sql_types::BigInt>)]
    task_id: Option<TaskId>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    task_name: Option<TaskName>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    queue: Option<QueueName>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Text>)]
    last_error_filter: Option<String>,
    #[diesel(sql_type = sql_types::Nullable<sql_types::BigInt>)]
    max_task_id: Option<i64>,
    #[diesel(sql_type = sql_types::BigInt)]
    affected: i64,
    #[diesel(sql_type = sql_types::Text)]
    operator: String,
    #[diesel(sql_type = sql_types::Text)]
    reason: String,
    #[diesel(sql_type = sql_types::Timestamptz)]
    at: DateTime<Utc>,
}

impl TryFrom<ActionRow> for AdminAction {
    type Error = SchedulerError;
    fn try_from(r: ActionRow) -> Result<Self, SchedulerError> {
        Ok(Self {
            id: r.id,
            action: r
                .action
                .parse()
                .map_err(|_: ParseEnumError| SchedulerError::Invariant("unknown admin action"))?,
            task_id: r.task_id,
            task_name: r.task_name,
            queue: r.queue,
            last_error_filter: r.last_error_filter,
            max_task_id: r.max_task_id,
            affected: r.affected,
            operator: r.operator,
            reason: r.reason,
            at: r.at,
        })
    }
}

const COLUMNS: &str = "id, action, task_id, task_name, queue, last_error_filter, max_task_id, \
                       affected, operator, reason, at";

/// Newest-first page of actions; pass the last id seen as `before` for the next page.
pub async fn admin_actions(
    conn: &mut AsyncPgConnection,
    before: Option<i64>,
    limit: i64,
) -> Result<Vec<AdminAction>, SchedulerError> {
    diesel::sql_query(format!(
        "SELECT {COLUMNS} FROM queue_admin_actions \
         WHERE ($1::bigint IS NULL OR id < $1) ORDER BY id DESC LIMIT $2"
    ))
    .bind::<sql_types::Nullable<sql_types::BigInt>, _>(before)
    .bind::<sql_types::BigInt, _>(limit)
    .get_results::<ActionRow>(conn)
    .await?
    .into_iter()
    .map(AdminAction::try_from)
    .collect()
}

/// Actions on one task: rows naming it, plus the (possibly bulk) action it links to.
pub async fn task_actions(
    conn: &mut AsyncPgConnection,
    task: &TaskView,
) -> Result<Vec<AdminAction>, SchedulerError> {
    diesel::sql_query(format!(
        "SELECT {COLUMNS} FROM queue_admin_actions WHERE task_id = $1 OR id = $2 \
         ORDER BY id DESC"
    ))
    .bind::<sql_types::BigInt, _>(task.id)
    .bind::<sql_types::Nullable<sql_types::BigInt>, _>(task.admin_action_id)
    .get_results::<ActionRow>(conn)
    .await?
    .into_iter()
    .map(AdminAction::try_from)
    .collect()
}
