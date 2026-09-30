WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.customer_id = 38
        )
)
-- Aggregate with conditional metrics
SELECT
    city,
    COUNT_IF(city = 'New York') AS new_york_events,
    COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count,
    COUNT(*) FILTER (WHERE country = 'SG') AS sg_rows,
    COUNT(*) FILTER (WHERE city = 'Tokyo') AS tokyo_rows,
    STDDEV_SAMP(age) AS age_stddev
FROM kept
GROUP BY city
HAVING COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) >= 1
ORDER BY new_york_events NULLS FIRST
