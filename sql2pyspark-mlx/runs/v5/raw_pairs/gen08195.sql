WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        prev_event_time,
        session_start_flag,
        MAX(session_id_2) OVER (PARTITION BY event_type, page) AS event_type_max_session_id_2,
        LAG(session_start_flag, 1) OVER (PARTITION BY event_type, session_id ORDER BY duration_sec, id) AS prev_session_start_flag,
        LEAD(session_start_flag, 1) OVER w_running AS next_session_start_flag
    FROM islands
    WHERE event_time >= '2023-04-01'
    WINDOW w_running AS (PARTITION BY page ORDER BY session_id_2, id)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        page,
        duration_sec,
        session_start_flag,
        prev_session_start_flag,
        FIRST_VALUE(session_start_flag, TRUE) OVER (ORDER BY prev_session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_start_flag,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY prev_event_time DESC, id) AS event_seq
    FROM metrics
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(page, 1, 4) AS page_prefix,
        event_seq - prev_session_start_flag AS event_seq_minus_prev_session_start_flag,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM enriched
    WHERE page IS DISTINCT FROM '/cart'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        page,
        first_known_session_start_flag,
        event_seq,
        page_prefix,
        page_filled
    FROM base
    QUALIFY RANK() OVER (PARTITION BY page ORDER BY event_seq_minus_prev_session_start_flag DESC NULLS LAST) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(event_seq) OVER (PARTITION BY page ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq,
        NTILE(4) OVER (ORDER BY first_known_session_start_flag, id) AS first_known_session_start_flag_bucket
    FROM ranked
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN running_event_seq >= 41 THEN 'High' WHEN running_event_seq >= 16 THEN 'Medium' ELSE 'Low' END AS running_event_seq_tier,
        CASE WHEN first_known_session_start_flag >= 1 THEN 'High' WHEN first_known_session_start_flag >= 0 THEN 'Medium' ELSE 'Low' END AS first_known_session_start_flag_band,
        COALESCE(page, 'OTHER') AS page_filled_2
    FROM with_windows
    WHERE page IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    SUM(CASE WHEN page = '/search' THEN first_known_session_start_flag_bucket ELSE 0 END) AS search_first_known_session_start_flag_bucket,
    MAX(first_known_session_start_flag_bucket) AS max_first_known_session_start_flag_bucket,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(event_seq) FILTER (WHERE page = '/product') AS product_avg_event_seq,
    MAX(first_known_session_start_flag) AS max_first_known_session_start_flag
FROM valid
GROUP BY CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(DISTINCT id) >= 1
ORDER BY max_first_known_session_start_flag NULLS FIRST;
