WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS(',', event_type, page) AS event_type_page_key
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-10-08 01:00:00'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        event_type_page_key,
        LAG(duration_sec, 2) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_count_30d,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_6_prior,
        NTILE(5) OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS duration_sec_bucket
    FROM filtered
),
enriched_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(duration_sec_bucket) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_bucket_sum_90d,
        COUNT(duration_sec) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_count_30d_2,
        AVG(duration_sec_avg_6_prior) OVER (PARTITION BY page ORDER BY duration_sec_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_6_prior_avg_6_prior
    FROM enriched
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN duration_sec_avg_6_prior_avg_6_prior >= 370 THEN 'High' WHEN duration_sec_avg_6_prior_avg_6_prior >= 190 THEN 'Medium' ELSE 'Low' END AS duration_sec_avg_6_prior_avg_6_prior_tier
    FROM enriched_web_events
    WHERE page IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(duration_sec) AS duration_sec_whole
    FROM cleaned
    WHERE event_time > TO_DATE('2024-04-01')
      AND page IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            session_id,
            event_type,
            burst_id
        FROM sessions_web_events
    ) src
    PIVOT (
        MAX(burst_id)
        FOR event_type IN ('purchase', 'search')
    )
)
SELECT
    session_id,
    purchase,
    search
FROM wide
ORDER BY search DESC;
