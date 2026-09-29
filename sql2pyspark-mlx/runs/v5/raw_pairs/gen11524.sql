WITH flagged AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
)
SELECT
    id,
    browser,
    started_at,
    SUM(new_group_flag) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM flagged;
