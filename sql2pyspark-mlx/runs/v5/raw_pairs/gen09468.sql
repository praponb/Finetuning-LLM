WITH with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p2.country,
        p2.device
    FROM web_events s
    JOIN sessions p2
        ON s.session_id = p2.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(duration_sec) OVER w_part running_max_duration_sec,
        COUNT(duration_sec) OVER w_part running_count_duration_sec,
        AVG(duration_sec) OVER w_part running_avg_duration_sec,
        COUNT(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) duration_sec_count_90d
    FROM with_dims
    WHERE device = 'mobile'
    WINDOW w_part AS (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_windows
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
with_windows_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        page,
        event_time,
        running_max_duration_sec,
        running_count_duration_sec,
        running_avg_duration_sec,
        duration_sec_count_90d,
        session_start_flag,
        island_id,
        SUM(running_max_duration_sec) OVER (PARTITION BY page ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_running_max_duration_sec,
        AVG(running_max_duration_sec) OVER (ORDER BY running_max_duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) running_max_duration_sec_avg_2_prior,
        NTILE(10) OVER (PARTITION BY session_id, country ORDER BY island_id, id) island_id_bucket,
        AVG(running_count_duration_sec) OVER (PARTITION BY country ORDER BY duration_sec_count_90d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) running_count_duration_sec_avg_6,
        FIRST_VALUE(running_max_duration_sec) IGNORE NULLS OVER (PARTITION BY event_type, session_id ORDER BY running_max_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_running_max_duration_sec
    FROM islands
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows_web_events
    EXCEPT ALL
    SELECT id
    FROM web_events
    WHERE page NOT IN ('/search', '/cart')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows_web_events s
    JOIN matched_keys k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    island_id,
    SUM(CASE WHEN page = '/cart' THEN first_known_running_max_duration_sec ELSE 0 END) AS cart_first_known_running_max_duration_sec,
    MAX(event_time) AS ended_at,
    GROUPING(DATE_TRUNC('MONTH', event_time)) AS grouping_level
FROM reconciled
GROUP BY ROLLUP (DATE_TRUNC('MONTH', event_time), island_id)
ORDER BY activity_month DESC
