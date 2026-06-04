DROP VIEW IF EXISTS scheduler_runs_status;
DROP TABLE IF EXISTS scheduler_run_outcomes;
DROP TABLE IF EXISTS scheduler_run_leases;
DROP TABLE IF EXISTS scheduler_runs;
DROP TABLE IF EXISTS scheduler_jobs;
DROP FUNCTION IF EXISTS scheduler_clear_lease_on_outcome;
DROP FUNCTION IF EXISTS scheduler_reject_lease_on_terminal;
DROP TYPE IF EXISTS run_outcome;
