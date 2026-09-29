WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(device) AS device_upper,
        COALESCE(country, 'DIRECT') AS country_clean
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-07-21 04:00:00'
      AND customer_id IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM kept
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.device_upper,
    s.country_clean,
    s.is_new_burst,
    s.session_id,
    CASE WHEN browser RLIKE '^(Chrome|Edge)$' THEN 'OTHER_ZONE' WHEN browser RLIKE '^(Safari|Firefox)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS browser_zone,
    (SELECT MIN(o2.discount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS min_order_discount
FROM islands s
ORDER BY id DESC NULLS LAST;
