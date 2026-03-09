insert into mart.support_efficiency (
    ticket_id,
    user_id,
    issue_type,
    status,
    created_at,
    updated_at,
    resolution_time_hours,
    message_count,
    is_open
)
select
    ticket_id,
    user_id,
    issue_type,
    status,
    created_at,
    updated_at,
    resolution_time_hours,
    message_count,
    is_open
from dwh.support_tickets
on conflict (ticket_id)
do update set
    user_id = excluded.user_id,
    issue_type = excluded.issue_type,
    status = excluded.status,
    created_at = excluded.created_at,
    updated_at = excluded.updated_at,
    resolution_time_hours = excluded.resolution_time_hours,
    message_count = excluded.message_count,
    is_open = excluded.is_open;