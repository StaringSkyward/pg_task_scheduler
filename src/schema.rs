diesel::table! {
    scheduler_jobs (id) {
        id -> diesel::sql_types::Uuid,
        name -> diesel::sql_types::Text,
        cron_expression -> diesel::sql_types::Text,
        job_args -> diesel::sql_types::Jsonb,
        next_run_at -> diesel::sql_types::Timestamptz,
        max_attempts -> diesel::sql_types::Integer,
        queue -> diesel::sql_types::Text,
        is_paused -> diesel::sql_types::Bool,
        created_at -> diesel::sql_types::Timestamptz,
        updated_at -> diesel::sql_types::Timestamptz,
    }
}
