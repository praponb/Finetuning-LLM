WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.page,
        s.event_time,
        s.duration_sec,
        b.browser,
        b.country,
        b.device
    FROM web_events s
    LEFT JOIN sessions b
        ON s.session_id = b.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        duration_sec,
        COUNT(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_count_90d,
        MAX(duration_sec) OVER w_hist AS browser_max_duration_sec
    FROM enriched
    WINDOW w_hist AS (PARTITION BY browser, device)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN duration_sec >= 390 THEN 'High' WHEN duration_sec >= 122 THEN 'Medium' ELSE 'Low' END AS duration_sec_band
    FROM metrics
    WHERE browser_max_duration_sec BETWEEN 329 AND 378
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(duration_sec) AS duration_sec_whole,
        browser_max_duration_sec * duration_sec_count_90d AS browser_max_duration_sec_times_duration_sec_count_90d
    FROM filtered
    WHERE duration_sec_count_90d >= 12
),
enriched_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (ORDER BY browser_max_duration_sec_times_duration_sec_count_90d DESC, id) AS browser_max_duration_sec_times_duration_sec_count_90d_bucket,
        LAST_VALUE(duration_sec_count_90d) IGNORE NULLS OVER (ORDER BY duration_sec_count_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec_count_90d,
        ROW_NUMBER() OVER (ORDER BY duration_sec_count_90d DESC, id) AS event_seq,
        NTILE(3) OVER (ORDER BY duration_sec, id) AS duration_sec_bucket,
        LAG(browser_max_duration_sec_times_duration_sec_count_90d, 2) OVER (ORDER BY duration_sec_whole, id) AS prev_browser_max_duration_sec_times_duration_sec_count_90d
    FROM cleaned
)
-- Multi-level aggregation
SELECT
    CASE WHEN duration_sec_whole >= 86 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_whole_tier,
    MAX(browser_max_duration_sec) AS max_browser_max_duration_sec,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids_2,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM enriched_web_events
GROUP BY CASE WHEN duration_sec_whole >= 86 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT(DISTINCT id) >= 1;
