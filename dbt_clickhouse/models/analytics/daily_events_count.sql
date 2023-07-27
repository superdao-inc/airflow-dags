{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "(tracker_id)"
   )
}}

select
    tracker_id,
    type as event_type,
    count() as count,
    toDateTime32(toDate(ceil(timestamp/1000))) as timestamp
from {{ source('analytics', 'tracker_events_prod') }}
where not empty(tracker_id)
group by tracker_id, event_type, timestamp