WITH flagged AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    browser,
    session_id,
    DATE_TRUNC('YEAR', started_at) AS year_start,
    ROUND(SUM(new_group_flag) / NULLIF(COUNT(CASE WHEN browser = 'Chrome' THEN 1 END), 0), 2) AS new_group_flag_per_chrome,
    COUNT_IF(browser = 'Chrome') AS chrome_events,
    ROUND(SUM(new_group_flag) / NULLIF(COUNT(CASE WHEN browser = 'Firefox' THEN 1 END), 0), 2) AS new_group_flag_per_firefox
FROM sessionized
GROUP BY GROUPING SETS ((browser, session_id, DATE_TRUNC('YEAR', started_at)), (browser, session_id), (browser))
