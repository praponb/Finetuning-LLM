WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(browser)) browser_title,
        LENGTH(browser) browser_length
    FROM sessions
    WHERE started_at >= TO_TIMESTAMP('2023-09-30 11:00:00')
      AND customer_id IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN browser_length >= 67 THEN 'HIGH' ELSE 'LOW' END AS browser_length_tier,
    customer_id,
    CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
    COUNT(*) FILTER (WHERE browser = 'Firefox') AS firefox_rows,
    COUNT_IF(country = 'SG') AS sg_events,
    SUM(session_no) FILTER (WHERE country = 'US') AS us_total_session_no,
    RANK() OVER (PARTITION BY CASE WHEN browser_length >= 67 THEN 'HIGH' ELSE 'LOW' END ORDER BY SUM(session_no) FILTER (WHERE country = 'US') DESC) AS us_total_session_no_rank
FROM islands
GROUP BY browser_length_tier, customer_id, session_start_flag_tier
HAVING SUM(session_no) FILTER (WHERE country = 'US') < 0
ORDER BY session_start_flag_tier DESC;
