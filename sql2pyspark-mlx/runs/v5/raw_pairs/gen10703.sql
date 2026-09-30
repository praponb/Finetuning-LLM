WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Grouped metrics
SELECT
    customer_id,
    ROUND(AVG(new_group_flag), 2) AS avg_new_group_flag,
    MIN(started_at) AS first_started_at,
    SUM(new_group_flag) AS total_new_group_flag,
    MIN(started_at) AS first_seen
FROM islands
GROUP BY customer_id
HAVING avg_new_group_flag < 1000000
ORDER BY customer_id DESC NULLS LAST
