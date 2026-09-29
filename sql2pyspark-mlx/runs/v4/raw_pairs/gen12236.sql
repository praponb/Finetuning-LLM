WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        COALESCE(country, 'N/A') AS country_filled
    FROM sessions
    WHERE browser IS NOT NULL
),
prev_events AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM base
),
gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    country,
    customer_id,
    ROUND(AVG(session_no), 2) AS avg_session_no,
    ROUND(AVG(session_no), 2) AS avg_session_no_2,
    APPROX_PERCENTILE(session_no, ARRAY(0.25, 0.5, 0.75)) AS session_no_quartiles,
    MIN(started_at) AS first_started_at
FROM islands
GROUP BY 1, 2
