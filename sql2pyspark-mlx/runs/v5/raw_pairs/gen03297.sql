WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY page) page_avg_duration_sec,
        PERCENT_RANK() OVER (PARTITION BY session_id, page ORDER BY duration_sec DESC) duration_sec_percent_rank
    FROM web_events
)
-- Aggregate with conditional metrics
SELECT
    event_type,
    SUM(duration_sec_percent_rank) AS total_duration_sec_percent_rank,
    AVG(duration_sec) FILTER (WHERE page = '/product') AS product_avg_duration_sec,
    APPROX_PERCENTILE(page_avg_duration_sec, 0.95) AS page_avg_duration_sec_p95
FROM with_windows
GROUP BY event_type
HAVING page_avg_duration_sec_p95 > 0;
