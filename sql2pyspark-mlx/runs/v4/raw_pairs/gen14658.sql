WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        DATEDIFF(CURRENT_DATE(), started_at) days_since_started_at,
        COALESCE(browser, 'OTHER') browser_filled
    FROM sessions
    WHERE started_at >= TO_DATE('2023-04-21')
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY days_since_started_at DESC) = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst_2
    FROM sessionized
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    PERCENTILE_APPROX(is_new_burst, 0.5) AS is_new_burst_p50,
    SIZE(COLLECT_SET(country)) AS num_countries,
    MAX(is_new_burst_2) FILTER (WHERE browser = 'Chrome') AS chrome_max_is_new_burst_2,
    ROUND(SUM(session_id) / NULLIF(COUNT(CASE WHEN browser = 'Chrome' THEN 1 END), 0), 2) AS session_id_per_chrome
FROM sessions_sessions
GROUP BY year_start
HAVING SIZE(COLLECT_SET(country)) < 1
