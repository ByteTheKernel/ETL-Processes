with checks as (

    select
        'user_sessions_null_session_id' as check_name,
        count(*)::int as error_count
    from dwh.user_sessions
    where session_id is null

    union all

    select
        'user_sessions_negative_duration' as check_name,
        count(*)::int as error_count
    from dwh.user_sessions
    where session_duration_seconds < 0

    union all

    select
        'user_sessions_invalid_time_range' as check_name,
        count(*)::int as error_count
    from dwh.user_sessions
    where end_time is not null
      and start_time > end_time

    union all

    select
        'event_logs_null_event_id' as check_name,
        count(*)::int as error_count
    from dwh.event_logs
    where event_id is null

    union all

    select
        'event_logs_null_event_type' as check_name,
        count(*)::int as error_count
    from dwh.event_logs
    where event_type is null

    union all

    select
        'support_tickets_null_ticket_id' as check_name,
        count(*)::int as error_count
    from dwh.support_tickets
    where ticket_id is null

    union all

    select
        'support_tickets_invalid_time_range' as check_name,
        count(*)::int as error_count
    from dwh.support_tickets
    where updated_at < created_at

    union all

    select
        'support_tickets_negative_resolution' as check_name,
        count(*)::int as error_count
    from dwh.support_tickets
    where resolution_time_hours is not null
      and resolution_time_hours < 0

    union all

    select
        'support_tickets_null_status' as check_name,
        count(*)::int as error_count
    from dwh.support_tickets
    where status is null

)
select *
from checks
order by check_name;