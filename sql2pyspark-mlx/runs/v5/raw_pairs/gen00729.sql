WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        CASE WHEN country = 'UK' THEN 1 ELSE 0 END AS is_uk
    FROM sessions
    WHERE started_at >= '2024-08-01'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_burst_tier,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN country = 'US' THEN is_uk ELSE 0 END) AS us_is_uk,
    ROUND(SUM(is_new_burst) / NULLIF(COUNT(CASE WHEN country = 'US' THEN 1 END), 0), 2) AS is_new_burst_per_us,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    SUM(CASE WHEN browser = 'Chrome' THEN is_new_burst ELSE 0 END) AS chrome_is_new_burst
FROM sessions_sessions
GROUP BY 1
HAVING COUNT(CASE WHEN country = 'SG' THEN 1 END) < 1000;
