WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(duration_sec, 0) AS duration_sec_rounded
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-09-04 15:00:00'
      AND page IS NOT DISTINCT FROM '/product'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec_rounded NULLS FIRST, id) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        b.browser,
        b.country
    FROM latest e
    LEFT JOIN sessions b
        ON e.session_id = b.id
    WHERE b.browser = 'Firefox'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY country ORDER BY row_num, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY country ORDER BY row_num, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY country ORDER BY row_num, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_duration_sec
    FROM enriched
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(last7_count_duration_sec) OVER (PARTITION BY browser, event_type ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_last7_count_duration_sec,
        COUNT(last7_count_duration_sec) OVER (PARTITION BY browser, event_type ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_last7_count_duration_sec,
        AVG(last7_count_duration_sec) OVER (PARTITION BY browser, event_type ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_last7_count_duration_sec
    FROM scored
)
SELECT
    id,
    event_time,
    duration_sec,
    duration_sec_rounded,
    row_num,
    browser,
    last7_count_duration_sec,
    last3_count_last7_count_duration_sec,
    last3_avg_last7_count_duration_sec,
    CASE WHEN browser RLIKE '^(Safari|Chrome)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS browser_zone
FROM metrics
