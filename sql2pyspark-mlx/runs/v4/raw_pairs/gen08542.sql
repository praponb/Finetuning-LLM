WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END AS is_purchase,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-09-12 15:00:00'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY page, event_type ORDER BY event_time DESC NULLS FIRST) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
latest_web_events AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY event_time DESC NULLS FIRST, id) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM latest_web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    COUNT(DISTINCT page) AS distinct_pages,
    ROUND(SUM(is_new_burst) / NULLIF(COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END), 0), 2) AS is_new_burst_per_add_to_cart,
    MAX(is_new_burst) FILTER (WHERE event_type = 'purchase') AS purchase_max_is_new_burst
FROM sessions_web_events
GROUP BY week_start
ORDER BY distinct_pages DESC
