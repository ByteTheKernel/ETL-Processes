with latest_sessions as (
    select
        s.session_id,
        s.source_doc,
        s.source_start_time,
        s.batch_id,
        row_number() over (
            partition by s.session_id
            order by s.source_start_time desc, s.id desc
        ) as rn
    from staging.user_sessions_raw s
),
prepared_sessions as (
    select
        session_id,
        source_doc,
        (source_doc ->> 'user_id') as user_id,
        ((source_doc ->> 'start_time')::timestamp) as start_time,
        ((source_doc ->> 'end_time')::timestamp) as end_time,
        extract(
            epoch from (
                ((source_doc ->> 'end_time')::timestamp) -
                ((source_doc ->> 'start_time')::timestamp)
            )
        )::int as session_duration_seconds,
        (source_doc -> 'device' ->> 'type') as device_type,
        (source_doc -> 'device' ->> 'os') as device_os,
        (source_doc -> 'device' ->> 'browser') as device_browser,
        jsonb_array_length(coalesce(source_doc -> 'pages_visited', '[]'::jsonb)) as pages_visited_count,
        jsonb_array_length(coalesce(source_doc -> 'actions', '[]'::jsonb)) as actions_count
    from latest_sessions
    where rn = 1
)
insert into dwh.user_sessions (
    session_id,
    user_id,
    start_time,
    end_time,
    session_duration_seconds,
    device_type,
    device_os,
    device_browser,
    pages_visited_count,
    actions_count,
    loaded_at,
    updated_at
)
select
    session_id,
    user_id,
    start_time,
    end_time,
    session_duration_seconds,
    device_type,
    device_os,
    device_browser,
    pages_visited_count,
    actions_count,
    current_timestamp,
    current_timestamp
from prepared_sessions
on conflict (session_id)
do update set
    user_id = excluded.user_id,
    start_time = excluded.start_time,
    end_time = excluded.end_time,
    session_duration_seconds = excluded.session_duration_seconds,
    device_type = excluded.device_type,
    device_os = excluded.device_os,
    device_browser = excluded.device_browser,
    pages_visited_count = excluded.pages_visited_count,
    actions_count = excluded.actions_count,
    updated_at = current_timestamp;
delete from dwh.session_pages
where session_id in (
    select distinct session_id
    from staging.user_sessions_raw
);

insert into dwh.session_pages (
    session_id,
    page_order,
    page_url,
    loaded_at
)
select
    s.session_id,
    p.ordinality::int as page_order,
    p.page_url,
    current_timestamp
from (
    select distinct on (session_id)
        session_id,
        source_doc,
        source_start_time,
        id
    from staging.user_sessions_raw
    order by session_id, source_start_time desc, id desc
) s
cross join lateral jsonb_array_elements_text(
    coalesce(s.source_doc -> 'pages_visited', '[]'::jsonb)
) with ordinality as p(page_url, ordinality);


delete from dwh.session_actions
where session_id in (
    select distinct session_id
    from staging.user_sessions_raw
);

insert into dwh.session_actions (
    session_id,
    action_order,
    action_name,
    loaded_at
)
select
    s.session_id,
    a.ordinality::int as action_order,
    a.action_name,
    current_timestamp
from (
    select distinct on (session_id)
        session_id,
        source_doc,
        source_start_time,
        id
    from staging.user_sessions_raw
    order by session_id, source_start_time desc, id desc
) s
cross join lateral jsonb_array_elements_text(
    coalesce(s.source_doc -> 'actions', '[]'::jsonb)
) with ordinality as a(action_name, ordinality);