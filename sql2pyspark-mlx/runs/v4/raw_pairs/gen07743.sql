WITH kept_keys AS (
    SELECT event_type, id
    FROM web_events
    INTERSECT
    SELECT event_type, id
    FROM web_events
    WHERE page NOT IN ('/product', '/checkout', '/home')
),
verified AS (
    SELECT s.*
    FROM web_events s
    INNER JOIN kept_keys k ON s.event_type = k.event_type AND s.id = k.id
)
SELECT
    CASE WHEN duration_sec >= 450 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    event_type,
    MAX(event_time) AS ended_at,
    APPROX_PERCENTILE(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles_2,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN event_type = 'page_view' THEN 1 END), 0), 2) AS duration_sec_per_page_view
FROM verified
GROUP BY CASE WHEN duration_sec >= 450 THEN 'HIGH' ELSE 'LOW' END, event_type
HAVING duration_sec_per_page_view >= 0
ORDER BY duration_sec_tier NULLS LAST
