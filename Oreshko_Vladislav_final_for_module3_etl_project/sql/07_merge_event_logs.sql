with latest_events as (
    select
        e.event_id,
        e.source_doc,
        e.source_timestamp,
        row_number() over (
            partition by e.event_id
            order by e.source_timestamp desc, e.id desc
        ) as rn
    from staging.event_logs_raw e
),
prepared_events as (
    select
        event_id,
        ((source_doc ->> 'timestamp')::timestamp) as event_timestamp,
        nullif(source_doc ->> 'event_type', '') as event_type,
        (source_doc -> 'details' ->> 'page') as detail_page,
        (source_doc -> 'details' ->> 'product_id') as detail_product_id,
        (source_doc -> 'details') as source_details
    from latest_events
    where rn = 1
)
insert into dwh.event_logs (
    event_id,
    event_timestamp,
    event_type,
    detail_page,
    detail_product_id,
    source_details,
    loaded_at,
    updated_at
)
select
    event_id,
    event_timestamp,
    event_type,
    detail_page,
    detail_product_id,
    source_details,
    current_timestamp,
    current_timestamp
from prepared_events
where event_type is not null
on conflict (event_id)
do update set
    event_timestamp = excluded.event_timestamp,
    event_type = excluded.event_type,
    detail_page = excluded.detail_page,
    detail_product_id = excluded.detail_product_id,
    source_details = excluded.source_details,
    updated_at = current_timestamp;