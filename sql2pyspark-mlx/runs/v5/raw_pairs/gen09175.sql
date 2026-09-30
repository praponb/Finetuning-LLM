WITH remaining_keys AS (
    SELECT city, id
    FROM suppliers
    INTERSECT ALL
    SELECT city, id
    FROM suppliers
    WHERE city NOT IN ('London', 'Berlin', 'Chiang Mai')
),
reconciled AS (
    SELECT s.*
    FROM suppliers s
    JOIN remaining_keys k ON s.city = k.city AND s.id = k.id
)
SELECT
    city,
    CASE WHEN rating >= 4.1 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    COUNT_IF(city = 'Chiang Mai') AS chiang_mai_events,
    SUM(rating) FILTER (WHERE country = 'TH') AS th_total_rating,
    COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows,
    STDDEV_SAMP(rating) AS rating_stddev,
    COUNT_IF(country = 'US') AS us_events,
    GROUPING(city) AS gid
FROM reconciled
GROUP BY ROLLUP (city, CASE WHEN rating >= 4.1 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY th_total_rating DESC NULLS LAST;
