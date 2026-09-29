WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY duration_sec, id) AS row_seq,
        SUM(duration_sec) OVER w_part AS running_duration_sec,
        MAX(duration_sec) OVER w_grp AS duration_sec_max_90d,
        LEAD(duration_sec, 2) OVER w_hist AS next_duration_sec,
        NTILE(10) OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS duration_sec_bucket
    FROM web_events
    WINDOW w_part AS (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY event_type ORDER BY event_time, id)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec_bucket DESC NULLS FIRST, id) AS dedup_rank
        FROM with_windows
    ) x
    WHERE dedup_rank = 1
),
with_dims AS (
    -- Attach sessions attributes
    SELECT e.*, b.browser
    FROM latest e
    LEFT JOIN sessions b
        ON e.session_id = b.id
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY browser ORDER BY event_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_dims
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_type,
        event_time,
        row_seq,
        duration_sec_bucket,
        browser,
        SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    id,
    event_type,
    event_time,
    row_seq,
    browser,
    session_no,
    CASE WHEN browser RLIKE '^(Firefox|Safari)$' THEN 'GROUP_B' WHEN browser RLIKE '^(Chrome|Edge)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS browser_zone
FROM sessionized;
