WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(page, 1, 3) AS page_prefix
    FROM web_events
    WHERE page IS NOT NULL
      AND event_time < '2025-09-01'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM filtered
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY page, event_type ORDER BY prev_event_time) AS dedup_rank
        FROM sessions_web_events
    ) t
    WHERE dedup_rank = 1
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        s.browser,
        s.device,
        s.country
    FROM deduped src
    INNER JOIN sessions s
        ON src.session_id = s.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        s.browser AS session_browser,
        s.country AS session_country
    FROM enriched e
    LEFT JOIN sessions s
        ON e.session_id = s.id
    WHERE s.customer_id = 10
),
deduped_web_events AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY dedup_rank DESC, id) = 1
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', prev_event_time) AS week_start,
        MAX(new_group_flag) FILTER (WHERE event_type = 'page_view') AS page_view_max_new_group_flag,
        AVG(new_group_flag) FILTER (WHERE page = '/home') AS home_avg_new_group_flag,
        SIZE(COLLECT_SET(session_country)) AS num_session_countries
    FROM deduped_web_events
    GROUP BY DATE_TRUNC('WEEK', prev_event_time)
)
SELECT week_start, page_view_max_new_group_flag
FROM totals
ORDER BY page_view_max_new_group_flag NULLS LAST;
