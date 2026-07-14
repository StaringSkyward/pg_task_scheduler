pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "scheduler_run_state"))]
    pub struct SchedulerRunState;

    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "scheduler_attempt_outcome"))]
    pub struct SchedulerAttemptOutcome;
}

diesel::table! {
    scheduler_jobs (id) {
        id -> diesel::sql_types::Uuid,
        name -> diesel::sql_types::Text,
        cron_expression -> diesel::sql_types::Text,
        job_args -> diesel::sql_types::Jsonb,
        next_run_at -> diesel::sql_types::Timestamptz,
        lease_duration -> diesel::sql_types::Interval,
        max_attempts -> diesel::sql_types::Integer,
        is_paused -> diesel::sql_types::Bool,
        created_at -> diesel::sql_types::Timestamptz,
        updated_at -> diesel::sql_types::Timestamptz,
        retry_backoff -> diesel::sql_types::Interval,
    }
}

diesel::table! {
    scheduler_runs (id) {
        id -> diesel::sql_types::Uuid,
        job_id -> diesel::sql_types::Nullable<diesel::sql_types::Uuid>,
        scheduled_for -> diesel::sql_types::Timestamptz,
        attempt_count -> diesel::sql_types::Integer,
        created_at -> diesel::sql_types::Timestamptz,
        updated_at -> diesel::sql_types::Timestamptz,
        job_name -> diesel::sql_types::Text,
        job_args -> diesel::sql_types::Jsonb,
        available_at -> diesel::sql_types::Timestamptz,
        priority -> diesel::sql_types::SmallInt,
        state -> crate::schema::sql_types::SchedulerRunState,
        max_attempts -> diesel::sql_types::Integer,
        lease_duration -> diesel::sql_types::Interval,
        retry_backoff -> diesel::sql_types::Interval,
        worker_id -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
        lease_token -> diesel::sql_types::Nullable<diesel::sql_types::Uuid>,
        lease_expires_at -> diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
        started_at -> diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
        finished_at -> diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>,
        last_error -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
        deduplication_key -> diesel::sql_types::Nullable<diesel::sql_types::Text>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::SchedulerAttemptOutcome;
    scheduler_run_attempts (run_id, attempt_number) {
        run_id -> Uuid,
        attempt_number -> Integer,
        worker_id -> Text,
        lease_token -> Uuid,
        started_at -> Timestamptz,
        lease_expires_at -> Timestamptz,
        finished_at -> Nullable<Timestamptz>,
        outcome -> Nullable<SchedulerAttemptOutcome>,
        error -> Nullable<Text>,
    }
}

diesel::joinable!(scheduler_run_attempts -> scheduler_runs (run_id));
diesel::allow_tables_to_appear_in_same_query!(
    scheduler_jobs,
    scheduler_runs,
    scheduler_run_attempts,
);
