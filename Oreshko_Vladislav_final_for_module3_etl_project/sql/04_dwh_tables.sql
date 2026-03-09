create table if not exists dwh.user_sessions (
    session_id text primary key,
    user_id text not null,
    start_time timestamp not null,
    end_time timestamp,
    session_duration_seconds int,
    device_type text,
    device_os text,
    device_browser text,
    pages_visited_count int default 0,
    actions_count int default 0,
    loaded_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);

create index if not exists idx_dwh_user_sessions_user_id
    on dwh.user_sessions (user_id);

create index if not exists idx_dwh_user_sessions_start_time
    on dwh.user_sessions (start_time);


create table if not exists dwh.session_pages (
    session_id text not null references dwh.user_sessions(session_id) on delete cascade,
    page_order int not null,
    page_url text not null,
    loaded_at timestamp default current_timestamp,
    primary key (session_id, page_order)
);

create index if not exists idx_dwh_session_pages_page_url
    on dwh.session_pages (page_url);


create table if not exists dwh.session_actions (
    session_id text not null references dwh.user_sessions(session_id) on delete cascade,
    action_order int not null,
    action_name text not null,
    loaded_at timestamp default current_timestamp,
    primary key (session_id, action_order)
);

create index if not exists idx_dwh_session_actions_action_name
    on dwh.session_actions (action_name);


create table if not exists dwh.event_logs (
    event_id text primary key,
    event_timestamp timestamp not null,
    event_type text not null,
    detail_page text,
    detail_product_id text,
    source_details jsonb,
    loaded_at timestamp default current_timestamp,
    updated_at timestamp default current_timestamp
);

create index if not exists idx_dwh_event_logs_event_timestamp
    on dwh.event_logs (event_timestamp);

create index if not exists idx_dwh_event_logs_event_type
    on dwh.event_logs (event_type);


create table if not exists dwh.support_tickets (
    ticket_id text primary key,
    user_id text not null,
    status text not null,
    issue_type text not null,
    created_at timestamp not null,
    updated_at timestamp not null,
    resolution_time_hours numeric(12,2),
    message_count int default 0,
    is_open boolean not null,
    loaded_at timestamp default current_timestamp,
    dwh_updated_at timestamp default current_timestamp
);

create index if not exists idx_dwh_support_tickets_user_id
    on dwh.support_tickets (user_id);

create index if not exists idx_dwh_support_tickets_status
    on dwh.support_tickets (status);

create index if not exists idx_dwh_support_tickets_issue_type
    on dwh.support_tickets (issue_type);


create table if not exists dwh.ticket_messages (
    ticket_id text not null references dwh.support_tickets(ticket_id) on delete cascade,
    message_order int not null,
    sender text,
    message_text text,
    message_timestamp timestamp,
    loaded_at timestamp default current_timestamp,
    primary key (ticket_id, message_order)
);

create index if not exists idx_dwh_ticket_messages_sender
    on dwh.ticket_messages (sender);

create index if not exists idx_dwh_ticket_messages_timestamp
    on dwh.ticket_messages (message_timestamp);