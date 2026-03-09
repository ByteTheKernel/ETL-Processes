create table if not exists mart.user_activity (
    user_id text primary key,
    total_sessions int not null,
    avg_session_duration_seconds numeric(12,2),
    total_pages_visited int not null,
    total_actions int not null,
    last_session_at timestamp,
    favorite_device_type text,
    updated_at timestamp default current_timestamp
);

create table if not exists mart.support_efficiency (
    ticket_id text primary key,
    user_id text not null,
    issue_type text not null,
    status text not null,
    created_at timestamp not null,
    updated_at timestamp not null,
    resolution_time_hours numeric(12,2),
    message_count int not null,
    is_open boolean not null
);