WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
islands AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    browser,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    ROUND(AVG(island_id), 2) AS avg_island_id,
    ROUND(SUM(island_id) / NULLIF(COUNT(CASE WHEN browser = 'Firefox' THEN 1 END), 0), 2) AS island_id_per_firefox
FROM islands
GROUP BY browser;
