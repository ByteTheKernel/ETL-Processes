create table if not exists staging.user_sessions_raw (
    id bigserial primary key,
    session_id text not null,
    source_doc jsonb not null,
    source_start_time timestamp,
    extracted_at timestamp default current_timestamp,
    batch_id text not null
);

create index if not exists idx_user_sessions_raw_session_id
    on staging.user_sessions_raw (session_id);

create index if not exists idx_user_sessions_raw_source_start_time
    on staging.user_sessions_raw (source_start_time);

create index if not exists idx_user_sessions_raw_batch_id
    on staging.user_sessions_raw (batch_id);


create table if not exists staging.event_logs_raw (
    id bigserial primary key,
    event_id text not null,
    source_doc jsonb not null,
    source_timestamp timestamp,
    extracted_at timestamp default current_timestamp,
    batch_id text not null
);

create index if not exists idx_event_logs_raw_event_id
    on staging.event_logs_raw (event_id);

create index if not exists idx_event_logs_raw_source_timestamp
    on staging.event_logs_raw (source_timestamp);

create index if not exists idx_event_logs_raw_batch_id
    on staging.event_logs_raw (batch_id);


create table if not exists staging.support_tickets_raw (
    id bigserial primary key,
    ticket_id text not null,
    source_doc jsonb not null,
    source_updated_at timestamp,
    extracted_at timestamp default current_timestamp,
    batch_id text not null
);

create index if not exists idx_support_tickets_raw_ticket_id
    on staging.support_tickets_raw (ticket_id);

create index if not exists idx_support_tickets_raw_source_updated_at
    on staging.support_tickets_raw (source_updated_at);

create index if not exists idx_support_tickets_raw_batch_id
    on staging.support_tickets_raw (batch_id);