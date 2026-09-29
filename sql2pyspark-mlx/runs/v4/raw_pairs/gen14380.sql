WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, event_type ORDER BY duration_sec DESC NULLS FIRST, id) AS latest_rank
        FROM web_events
    ) ranked
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec >= 492 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    MIN(event_time) AS first_seen,
    GROUPING_ID() AS gid
FROM deduped
GROUP BY CUBE (CASE WHEN duration_sec >= 492 THEN 'HIGH' ELSE 'LOW' END);
