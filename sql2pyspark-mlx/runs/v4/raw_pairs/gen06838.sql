WITH recent_suppliers AS (
    SELECT *, COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE rating > 2.4
),
agg AS (
    SELECT country, CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(*) AS cnt
    FROM recent_suppliers
    GROUP BY country, CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM agg;
