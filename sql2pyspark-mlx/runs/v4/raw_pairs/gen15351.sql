WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(event_type) clean_event_type
    FROM web_events
    WHERE page IS NOT NULL
)
-- Ranking and gap features
SELECT
    *,
    SUM(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) duration_sec_sum_7d,
    PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY duration_sec NULLS FIRST) duration_sec_percent_rank
FROM cleaned
