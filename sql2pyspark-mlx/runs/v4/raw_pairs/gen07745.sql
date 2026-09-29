WITH with_prev AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
with_gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    CASE WHEN island_id >= 7 THEN 'HIGH' ELSE 'LOW' END AS island_id_tier,
    CASE WHEN is_new_burst >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_new_burst_tier,
    MAX(prev_started_at) AS last_seen,
    MAX(started_at) AS last_seen_2,
    SUM(CASE WHEN browser = 'Chrome' THEN island_id ELSE 0 END) AS chrome_island_id,
    SUM(CASE WHEN browser = 'Edge' THEN is_new_burst ELSE 0 END) AS edge_is_new_burst,
    MIN(started_at) AS first_started_at,
    (UNIX_TIMESTAMP(MAX(prev_started_at)) - UNIX_TIMESTAMP(MIN(prev_started_at))) / 60 AS span_minutes
FROM islands
GROUP BY CASE WHEN island_id >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN is_new_burst >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING chrome_island_id > 0
ORDER BY island_id_tier DESC NULLS LAST;
