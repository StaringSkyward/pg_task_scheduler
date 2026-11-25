diesel::table! {
    scheduler_jobs (id) {
        id -> diesel::sql_types::Uuid,
        name -> diesel::sql_types::Text,
        task_name -> diesel::sql_types::Text,
        cron_expression -> diesel::sql_types::Text,
        job_args -> diesel::sql_types::Jsonb,
        max_attempts -> diesel::sql_types::Integer,
        queue -> diesel::sql_types::Text,
        created_at -> diesel::sql_types::Timestamptz,
        updated_at -> diesel::sql_types::Timestamptz,
    }
}

diesel::table! {
    scheduler_job_pauses (job_id) {
        job_id -> diesel::sql_types::Uuid,
        paused_at -> diesel::sql_types::Timestamptz,
    }
}

diesel::table! {
    scheduler_job_cursors (job_id) {
        job_id -> diesel::sql_types::Uuid,
        next_run_at -> diesel::sql_types::Timestamptz,
    }
}

diesel::joinable!(scheduler_job_pauses -> scheduler_jobs (job_id));
diesel::joinable!(scheduler_job_cursors -> scheduler_jobs (job_id));
diesel::allow_tables_to_appear_in_same_query!(
    scheduler_jobs,
    scheduler_job_pauses,
    scheduler_job_cursors
);
