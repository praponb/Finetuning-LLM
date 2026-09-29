WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(page) IGNORE NULLS OVER w_ordered AS last_known_page,
        SUM(duration_sec) OVER w_ordered AS running_duration_sec
    FROM web_events
    WINDOW w_ordered AS (ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    event_type,
    page,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    MAX(event_time) AS ended_at,
    PERCENTILE_APPROX(running_duration_sec, 0.95) AS running_duration_sec_p95,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    COUNT(*) AS num_rows,
    SUM(duration_sec) AS total_duration_sec,
    MAX(event_time) AS ended_at_2
FROM metrics
GROUP BY event_type, page, year_start
ORDER BY event_type DESC NULLS LAST
