WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY event_type) AS event_type_count_duration_sec,
        PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS duration_sec_percent_rank_2
    FROM web_events
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_percent_rank_2 DESC NULLS FIRST, id) = 1
),
joined AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        b.browser,
        b.device,
        b.country
    FROM ranked e
    LEFT JOIN sessions b
        ON e.session_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    CASE WHEN duration_sec >= 521 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    PERCENTILE_APPROX(duration_sec_percent_rank, 0.5) AS duration_sec_percent_rank_p50,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    COUNT(DISTINCT browser) AS distinct_browsers,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    PERCENTILE_APPROX(duration_sec_percent_rank_2, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_percent_rank_2_quartiles,
    COUNT(*) AS event_count
FROM joined
GROUP BY ROLLUP (DATE_TRUNC('WEEK', event_time), CASE WHEN duration_sec >= 521 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY event_count DESC;
