WITH with_gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    CASE WHEN island_id >= 8 THEN 'HIGH' ELSE 'LOW' END AS island_id_tier,
    SUM(duration_sec) AS total_duration_sec,
    COUNT(DISTINCT session_id) AS distinct_session_ids,
    COUNT_IF(page = '/home') AS home_events,
    SUM(CASE WHEN event_type = 'purchase' THEN duration_sec ELSE 0 END) AS purchase_duration_sec,
    ROUND(SUM(island_id) / NULLIF(COUNT(CASE WHEN event_type = 'search' THEN 1 END), 0), 2) AS island_id_per_search,
    COUNT(CASE WHEN event_type = 'search' THEN 1 END) AS search_count
FROM islands
GROUP BY 1
HAVING island_id_per_search > 0
ORDER BY island_id_tier DESC NULLS FIRST
