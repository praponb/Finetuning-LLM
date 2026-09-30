WITH base AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CONCAT(event_type, '/', page) AS event_type_page,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-08-24 09:00:00'
      AND page IS NOT NULL
),
metrics AS (
    SELECT
        *,
        LAG(duration_sec, 2) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_duration_sec,
        STDDEV_SAMP(duration_sec) OVER (PARTITION BY page) AS page_stddev_samp_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_avg_3,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_avg_2
    FROM base
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time DESC NULLS FIRST) AS dedup_rank
        FROM metrics
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    SIZE(COLLECT_SET(page)) AS num_pages,
    COUNT(CASE WHEN page = '/search' THEN 1 END) AS search_count
FROM ranked
GROUP BY DATE_TRUNC('WEEK', event_time)
ORDER BY search_count DESC NULLS LAST
