WITH latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.country,
        x.event_type
    FROM sessions s
    JOIN LATERAL (
            SELECT c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    event_type,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(event_type = 'purchase') AS purchase_events,
    COUNT_IF(country = 'TH') AS th_events,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list_2,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM latest_match
GROUP BY event_type
HAVING distinct_ids < 1000
ORDER BY th_events NULLS FIRST
