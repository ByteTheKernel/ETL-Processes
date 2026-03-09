create table if not exists meta.etl_watermarks (
    source_name text primary key,
    watermark_field text not null,
    last_watermark_value timestamp,
    last_run_at timestamp,
    last_status text
);

create table if not exists meta.etl_job_log (
    id bigserial primary key,
    job_name text not null,
    source_name text,
    run_id text,
    batch_id text,
    rows_extracted int default 0,
    rows_loaded_staging int default 0,
    rows_merged_dwh int default 0,
    status text,
    error_message text,
    started_at timestamp default current_timestamp,
    finished_at timestamp
);

insert into meta.etl_watermarks (source_name, watermark_field, last_watermark_value, last_run_at, last_status)
values
    ('user_sessions', 'start_time', null, null, 'never_run'),
    ('event_logs', 'timestamp', null, null, 'never_run'),
    ('support_tickets', 'updated_at', null, null, 'never_run')
on conflict (source_name) do nothing;