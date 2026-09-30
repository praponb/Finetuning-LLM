WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        country,
        DATE_TRUNC('year', started_at) AS started_at_period,
        TO_DATE(started_at) AS started_at_date
    FROM sessions
    WHERE started_at > TO_DATE('2023-06-26')
      AND customer_id = 40
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at_period) OVER (PARTITION BY country ORDER BY started_at_period, id) AS prev_started_at_period
    FROM valid
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at_period IS NULL OR started_at_period > prev_started_at_period + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', started_at_period) AS year_start,
    COUNT_IF(country = 'US') AS us_events,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    COUNT(*) FILTER (WHERE country = 'DE') AS de_rows,
    (UNIX_TIMESTAMP(MAX(started_at_period)) - UNIX_TIMESTAMP(MIN(started_at_period))) / 60 AS duration_min
FROM islands
GROUP BY 1
ORDER BY year_start DESC NULLS FIRST;
