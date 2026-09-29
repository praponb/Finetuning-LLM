WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(page) AS clean_page
    FROM web_events
    WHERE event_time > TO_DATE('2023-07-31')
      AND page IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM deduped
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.duration_sec,
        s.clean_page,
        s.latest_rank,
        s.prev_event_time,
        d.browser,
        d.country,
        d.device
    FROM islands s
    JOIN sessions d
        ON s.session_id = d.id
    WHERE d.browser IS NULL
)
-- Grouped metrics
SELECT
    device,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    (UNIX_TIMESTAMP(MAX(prev_event_time)) - UNIX_TIMESTAMP(MIN(prev_event_time))) / 60 AS duration_min,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count
FROM joined
GROUP BY device
ORDER BY tablet_count DESC NULLS LAST
