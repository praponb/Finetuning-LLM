WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN page IN ('/home', '/cart') THEN 'Core' ELSE 'Other' END AS page_group,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-03-18 06:00:00'
      AND event_type <> 'search'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(duration_sec) OVER w_grp AS prev_duration_sec,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM filtered
    WINDOW w_grp AS (PARTITION BY event_type ORDER BY event_time, id)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(event_seq * 0.9, 2) AS event_seq_scaled,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM scored
    WHERE event_time >= TIMESTAMP '2023-03-25 17:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time, id) AS seq_no,
        MAX(prev_duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS prev_duration_sec_max_90d,
        LEAD(event_seq, 1) OVER (ORDER BY event_time, id) AS next_event_seq
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(event_seq_scaled, TRUE) OVER (PARTITION BY session_id ORDER BY next_event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq_scaled,
        AVG(seq_no) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS seq_no_avg_2,
        RANK() OVER (ORDER BY next_event_seq DESC) AS next_event_seq_rank
    FROM islands
    WHERE seq_no = 7
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_burst_tier,
        CASE WHEN seq_no >= 7 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
        DATE_TRUNC('WEEK', event_time) AS week_start,
        SUM(CASE WHEN event_type = 'search' THEN is_new_burst ELSE 0 END) AS search_is_new_burst,
        COUNT(CASE WHEN page = '/product' THEN 1 END) AS product_count,
        SIZE(COLLECT_SET(page)) AS num_pages,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
        GROUPING_ID() AS gid
    FROM with_windows
    GROUP BY GROUPING SETS ((CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN seq_no >= 7 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', event_time)), (CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN seq_no >= 7 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END), ())
)
SELECT
    seq_no_tier,
    week_start,
    search_is_new_burst,
    avg_duration_sec,
    gid
FROM summary
ORDER BY seq_no_tier DESC NULLS FIRST
