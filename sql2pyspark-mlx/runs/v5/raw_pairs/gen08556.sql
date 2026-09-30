WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY session_id_2, id) AS dedup_rank
    FROM sessions_web_events
    QUALIFY dedup_rank = 1
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        p.device,
        p.browser
    FROM ranked src
    INNER JOIN sessions p
        ON src.session_id = p.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_type,
        duration_sec,
        dedup_rank,
        browser,
        NTILE(5) OVER (PARTITION BY session_id ORDER BY session_id_2 DESC, id) AS session_id_2_bucket,
        SUM(is_new_burst) OVER (PARTITION BY device ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_new_burst_sum_7d
    FROM joined
    WHERE dedup_rank = 1
),
enriched_web_events AS (
    -- Running and moving aggregates
    SELECT
        id,
        duration_sec,
        browser,
        session_id_2_bucket,
        LAG(duration_sec, 1) OVER (PARTITION BY browser ORDER BY session_id_2_bucket, id) AS prev_duration_sec,
        SUM(duration_sec) OVER w_ordered AS cumulative_duration_sec
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY browser, event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER w_ordered AS duration_sec_bucket,
        LAG(cumulative_duration_sec, 1) OVER (PARTITION BY browser ORDER BY cumulative_duration_sec, id) AS prev_cumulative_duration_sec
    FROM enriched_web_events
    WINDOW w_ordered AS (PARTITION BY browser ORDER BY duration_sec DESC, id)
),
daily AS (
    -- Grouped metrics
    SELECT
        browser,
        COUNT(*) FILTER (WHERE browser = 'Safari') AS safari_rows,
        SIZE(COLLECT_SET(browser)) AS num_browsers,
        SUM(CASE WHEN browser = 'Edge' THEN duration_sec_bucket ELSE 0 END) AS edge_duration_sec_bucket,
        SIZE(COLLECT_SET(browser)) AS num_browsers_2,
        SORT_ARRAY(COLLECT_SET(browser)) AS browser_list
    FROM metrics
    GROUP BY browser
)
SELECT
    browser,
    edge_duration_sec_bucket,
    num_browsers_2,
    CASE WHEN browser RLIKE '^(Edge|Safari)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS browser_zone
FROM daily
ORDER BY browser_zone NULLS LAST, edge_duration_sec_bucket
