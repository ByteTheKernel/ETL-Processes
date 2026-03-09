with latest_tickets as (
    select
        t.ticket_id,
        t.source_doc,
        t.source_updated_at,
        row_number() over (
            partition by t.ticket_id
            order by t.source_updated_at desc, t.id desc
        ) as rn
    from staging.support_tickets_raw t
),
prepared_tickets as (
    select
        ticket_id,
        (source_doc ->> 'user_id') as user_id,
        (source_doc ->> 'status') as status,
        (source_doc ->> 'issue_type') as issue_type,
        ((source_doc ->> 'created_at')::timestamp) as created_at,
        ((source_doc ->> 'updated_at')::timestamp) as updated_at,
        case
            when (source_doc ->> 'status') in ('resolved', 'closed')
            then round(
                extract(
                    epoch from (
                        ((source_doc ->> 'updated_at')::timestamp) -
                        ((source_doc ->> 'created_at')::timestamp)
                    )
                ) / 3600.0,
                2
            )
            else null
        end as resolution_time_hours,
        jsonb_array_length(coalesce(source_doc -> 'messages', '[]'::jsonb)) as message_count,
        case
            when (source_doc ->> 'status') in ('open', 'in_progress') then true
            else false
        end as is_open,
        source_doc
    from latest_tickets
    where rn = 1
)
insert into dwh.support_tickets (
    ticket_id,
    user_id,
    status,
    issue_type,
    created_at,
    updated_at,
    resolution_time_hours,
    message_count,
    is_open,
    loaded_at,
    dwh_updated_at
)
select
    ticket_id,
    user_id,
    status,
    issue_type,
    created_at,
    updated_at,
    resolution_time_hours,
    message_count,
    is_open,
    current_timestamp,
    current_timestamp
from prepared_tickets
on conflict (ticket_id)
do update set
    user_id = excluded.user_id,
    status = excluded.status,
    issue_type = excluded.issue_type,
    created_at = excluded.created_at,
    updated_at = excluded.updated_at,
    resolution_time_hours = excluded.resolution_time_hours,
    message_count = excluded.message_count,
    is_open = excluded.is_open,
    dwh_updated_at = current_timestamp;


delete from dwh.ticket_messages
where ticket_id in (
    select distinct ticket_id
    from staging.support_tickets_raw
);

insert into dwh.ticket_messages (
    ticket_id,
    message_order,
    sender,
    message_text,
    message_timestamp,
    loaded_at
)
select
    t.ticket_id,
    m.ordinality::int as message_order,
    m.message_item ->> 'sender' as sender,
    m.message_item ->> 'message_text' as message_text,
    (m.message_item ->> 'message_timestamp')::timestamp as message_timestamp,
    current_timestamp
from (
    select distinct on (ticket_id)
        ticket_id,
        source_doc,
        source_updated_at,
        id
    from staging.support_tickets_raw
    order by ticket_id, source_updated_at desc, id desc
) t
cross join lateral jsonb_array_elements(
    coalesce(t.source_doc -> 'messages', '[]'::jsonb)
) with ordinality as m(message_item, ordinality);