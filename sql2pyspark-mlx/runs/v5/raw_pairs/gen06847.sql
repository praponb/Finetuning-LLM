WITH with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        s.browser,
        s.device
    FROM web_events e
    INNER JOIN sessions s
        ON e.session_id = s.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        LAG(duration_sec, 1) OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS prev_duration_sec
    FROM with_dims
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(prev_duration_sec) OVER w_recent AS running_max_prev_duration_sec,
        MIN(prev_duration_sec) OVER w_recent AS running_min_prev_duration_sec,
        COUNT(prev_duration_sec) OVER w_recent AS running_count_prev_duration_sec,
        SUM(prev_duration_sec) OVER w_recent AS running_sum_prev_duration_sec
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.page,
        e.duration_sec,
        e.browser,
        e.running_duration_sec,
        e.running_min_prev_duration_sec,
        e.running_sum_prev_duration_sec,
        d.device AS session_device
    FROM enriched e
    INNER JOIN sessions d
        ON e.session_id = d.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        browser,
        session_device,
        running_duration_sec
    FROM enriched_web_events
) src
PIVOT (
    SUM(running_duration_sec) AS total,
    COUNT(1) AS item_count
    FOR session_device IN ('mobile', 'tablet')
)
