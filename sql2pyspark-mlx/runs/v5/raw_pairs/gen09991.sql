WITH filtered AS (
    SELECT
        *,
        DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-07-07 08:00:00'
      AND page IS DISTINCT FROM '/search'
),
with_gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessionized AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        page_filled,
        SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    CASE WHEN burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_tier,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    PERCENTILE_APPROX(duration_sec, 0.95) AS duration_sec_p95,
    MIN(event_time) AS started_at,
    GROUPING(CASE WHEN burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_id
FROM sessionized
GROUP BY GROUPING SETS ((CASE WHEN burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', event_time)), (CASE WHEN burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY duration_sec_p95 DESC
