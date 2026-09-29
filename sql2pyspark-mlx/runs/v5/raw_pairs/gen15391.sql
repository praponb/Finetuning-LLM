WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    customer_id,
    browser,
    started_at,
    session_id,
    CASE WHEN browser RLIKE '^(Edge|Chrome)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS browser_zone
FROM sessionized
ORDER BY session_id DESC
