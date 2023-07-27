{{
   config(
        materialized = "table",
        engine = 'MergeTree()',
        order_by = "(tracker_id)"
   )
}}

WITH
first_wallet_connects AS (
    SELECT
        tracker_id,
        lower(user_wallet_address)   AS address,
        first_value(timestamp)       AS timestamp,
        first_value(page_utm_source) AS source,
        type                         AS event
    FROM {{ source('analytics', 'tracker_events_prod') }}
    WHERE type = 'WALLET_CONNECT'
    AND user_wallet_address IS NOT NULL
    GROUP BY tracker_id, address, event
),
first_form_submits AS (
    SELECT
        tracker_id,
        lower(user_wallet_address)   AS address,
        first_value(timestamp)       AS timestamp,
        first_value(page_utm_source) AS source,
        type                         AS event
    FROM {{ source('analytics', 'tracker_events_prod') }}
    WHERE type = 'FORM_SUBMIT'
    AND user_wallet_address IS NOT NULL
    GROUP BY tracker_id, address, event
)
SELECT
    tracker_id,
    address,
    last_value(event) AS last_event,
    toDateTime(ceil(last_value(timestamp)/1000)) AS last_event_timestamp,
    last_value(source) AS source,
    now() AS updated
FROM (
    SELECT * FROM first_wallet_connects
    UNION ALL
    SELECT * FROM first_form_submits
)
GROUP BY tracker_id, address