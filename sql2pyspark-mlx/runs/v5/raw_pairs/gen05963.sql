WITH remaining_keys AS (
    SELECT city, id
    FROM suppliers
    INTERSECT
    SELECT city, id
    FROM suppliers
    WHERE country IN ('UK', 'DE')
),
reconciled AS (
    SELECT s.*
    FROM suppliers s
    JOIN remaining_keys k ON s.city = k.city AND s.id = k.id
)
SELECT
    country,
    CASE WHEN rating >= 3.7 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    COUNT(*) AS cnt,
    COUNT_IF(city = 'Tokyo') AS tokyo_events
FROM reconciled
GROUP BY country, CASE WHEN rating >= 3.7 THEN 'HIGH' ELSE 'LOW' END
ORDER BY tokyo_events DESC NULLS LAST
