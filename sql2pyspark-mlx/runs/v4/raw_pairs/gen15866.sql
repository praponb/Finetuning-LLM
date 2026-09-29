WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(country, ', ', device) AS country_device
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-06-28 11:00:00'
      AND device IN ('tablet', 'desktop', 'mobile')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        country_device,
        LAST_VALUE(browser, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        LAST_VALUE(browser, TRUE) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser_2
    FROM filtered
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_start_flag_tier,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(burst_id, 0.95) AS burst_id_p95
FROM islands
GROUP BY CASE WHEN session_start_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING burst_id_p95 >= 0;
