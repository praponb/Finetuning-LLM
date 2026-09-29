WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions
)
-- Running sum of the flag = session number
SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
FROM flagged
