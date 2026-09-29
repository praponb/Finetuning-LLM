WITH latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.web_event_page
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) AS prev_started_at
    FROM latest_match
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
islands_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, browser ORDER BY started_at NULLS LAST, id) AS rank_in_group
        FROM islands_sessions
    ) x
    WHERE rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    burst_id,
    COUNT_IF(device = 'desktop') AS desktop_events,
    COUNT(*) AS cnt,
    SORT_ARRAY(COLLECT_SET(web_event_page)) AS web_event_page_list
FROM ranked
GROUP BY 1
HAVING desktop_events < 1
ORDER BY burst_id DESC NULLS FIRST;
