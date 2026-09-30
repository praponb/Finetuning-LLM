WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS rank_in_group
    FROM web_events
    QUALIFY rank_in_group = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, session_id
    FROM deduped
    EXCEPT DISTINCT
    SELECT id, session_id
    FROM web_events
    WHERE event_time >= '2024-05-03'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.session_id = k.session_id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        rank_in_group,
        FLOOR(duration_sec) AS duration_sec_whole
    FROM reconciled
    WHERE event_time >= TIMESTAMP '2024-01-19 12:00:00'
      AND page IS NOT NULL
      AND event_time < '2023-08-01'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    CASE WHEN duration_sec_whole >= 67 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_whole_tier,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    SUM(CASE WHEN page = '/checkout' THEN duration_sec_whole ELSE 0 END) AS checkout_duration_sec_whole,
    SIZE(COLLECT_SET(event_type)) AS num_event_types_2,
    SUM(duration_sec_whole) FILTER (WHERE event_type = 'purchase') AS purchase_total_duration_sec_whole
FROM filtered
GROUP BY DATE_TRUNC('WEEK', event_time), CASE WHEN duration_sec_whole >= 67 THEN 'HIGH' ELSE 'LOW' END
