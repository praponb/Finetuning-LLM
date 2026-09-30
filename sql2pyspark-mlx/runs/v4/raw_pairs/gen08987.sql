WITH kept_keys AS (
    SELECT event_type
    FROM web_events
    EXCEPT
    SELECT event_type
    FROM web_events
    WHERE event_time < '2024-01-01'
),
verified AS (
    SELECT s.*
    FROM web_events s
    INNER JOIN kept_keys k ON s.event_type = k.event_type
)
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    COUNT(*) FILTER (WHERE page = '/cart') AS cart_rows,
    COUNT(*) FILTER (WHERE event_type = 'click') AS click_rows,
    SUM(duration_sec) FILTER (WHERE page = '/search') AS search_total_duration_sec,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN page = '/search' THEN 1 END), 0), 2) AS duration_sec_per_search,
    PERCENTILE_APPROX(duration_sec, 0.5) AS duration_sec_p50
FROM verified
GROUP BY DATE_TRUNC('YEAR', event_time);
