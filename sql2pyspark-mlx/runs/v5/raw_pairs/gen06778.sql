WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        CONCAT(page, ', ', event_type) AS page_event_type,
        LOWER(event_type) AS event_type_lower
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-04-29 10:00:00'
      AND page IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT session_id, id
    FROM islands
    INTERSECT
    SELECT session_id, id
    FROM web_events
    WHERE duration_sec BETWEEN 147 AND 372
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    JOIN remaining_keys k ON s.session_id = k.session_id AND s.id = k.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(session_no) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS session_no_avg_30d,
        MIN(duration_sec) OVER (PARTITION BY event_type, session_id) AS event_type_min_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_sum_90d,
        CUME_DIST() OVER (PARTITION BY session_id ORDER BY duration_sec NULLS FIRST) AS duration_sec_cume_dist
    FROM kept_rows
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(duration_sec, 0) AS duration_sec_rounded
    FROM enriched
    WHERE session_no_avg_30d <= 2
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.session_id,
        src.event_type,
        src.event_time,
        src.duration_sec,
        src.page_event_type,
        src.event_type_lower,
        src.session_start_flag,
        src.session_no,
        src.duration_sec_sum_90d,
        src.duration_sec_cume_dist,
        src.duration_sec_rounded,
        b.device,
        b.country
    FROM base src
    JOIN sessions b
        ON src.session_id = b.id
    WHERE b.browser = 'Firefox'
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            session_id,
            event_type,
            duration_sec_sum_90d
        FROM with_dims
    ) src
    PIVOT (
        MAX(duration_sec_sum_90d) AS amount,
        COUNT(1) AS n
        FOR event_type IN ('purchase' AS purchase, 'page_view' AS page_view)
    )
)
SELECT
    s.session_id,
    s.purchase_amount,
    s.page_view_amount,
    s.page_view_n,
    (SELECT SUM(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS sum_web_event_duration_sec
FROM pivoted s
ORDER BY page_view_n DESC NULLS LAST;
