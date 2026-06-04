pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "run_outcome"))]
    pub struct RunOutcome;
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
    }
}

diesel::table! {
    scheduler_runs (id) {
        id -> diesel::sql_types::Uuid,
        job_id -> diesel::sql_types::Uuid,
        scheduled_for -> diesel::sql_types::Timestamptz,
        attempt_count -> diesel::sql_types::Integer,
        created_at -> diesel::sql_types::Timestamptz,
        updated_at -> diesel::sql_types::Timestamptz,
    }
}

diesel::table! {
    scheduler_run_leases (run_id) {
        run_id -> diesel::sql_types::Uuid,
        worker_id -> diesel::sql_types::Text,
        lease_token -> diesel::sql_types::Uuid,
        lease_expires_at -> diesel::sql_types::Timestamptz,
        started_at -> diesel::sql_types::Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::RunOutcome;
    scheduler_run_outcomes (run_id) {
        run_id -> Uuid,
        outcome -> RunOutcome,
        finished_at -> Timestamptz,
        last_error -> Nullable<Text>,
    }
}

diesel::joinable!(scheduler_runs -> scheduler_jobs (job_id));
diesel::joinable!(scheduler_run_leases -> scheduler_runs (run_id));
diesel::joinable!(scheduler_run_outcomes -> scheduler_runs (run_id));
diesel::allow_tables_to_appear_in_same_query!(
    scheduler_jobs,
    scheduler_runs,
    scheduler_run_leases,
    scheduler_run_outcomes,
);
