insert into mart.user_activity (
    user_id,
    total_sessions,
    avg_session_duration_seconds,
    total_pages_visited,
    total_actions,
    last_session_at,
    favorite_device_type,
    updated_at
)
with device_ranked as (
    select
        user_id,
        device_type,
        count(*) as device_cnt,
        row_number() over (
            partition by user_id
            order by count(*) desc, device_type
        ) as rn
    from dwh.user_sessions
    group by user_id, device_type
),
favorite_device as (
    select
        user_id,
        device_type as favorite_device_type
    from device_ranked
    where rn = 1
)
select
    us.user_id,
    count(*) as total_sessions,
    round(avg(us.session_duration_seconds)::numeric, 2) as avg_session_duration_seconds,
    coalesce(sum(us.pages_visited_count), 0) as total_pages_visited,
    coalesce(sum(us.actions_count), 0) as total_actions,
    max(us.start_time) as last_session_at,
    fd.favorite_device_type,
    current_timestamp
from dwh.user_sessions us
left join favorite_device fd
    on us.user_id = fd.user_id
group by us.user_id, fd.favorite_device_type
on conflict (user_id)
do update set
    total_sessions = excluded.total_sessions,
    avg_session_duration_seconds = excluded.avg_session_duration_seconds,
    total_pages_visited = excluded.total_pages_visited,
    total_actions = excluded.total_actions,
    last_session_at = excluded.last_session_at,
    favorite_device_type = excluded.favorite_device_type,
    updated_at = current_timestamp;