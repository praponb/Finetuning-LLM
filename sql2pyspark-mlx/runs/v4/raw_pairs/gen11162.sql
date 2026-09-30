WITH recent_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name NOT LIKE 'G%' AND city NOT IN ('Tokyo', 'Singapore')
),
suppliers_stats AS (
    SELECT
        country,
        CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END rating_bucket,
        ROUND(AVG(rating), 2) avg_rating,
        MAX(rating) max_rating
    FROM recent_suppliers
    GROUP BY country, CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM suppliers_stats
WHERE avg_rating > 46
ORDER BY rating_bucket DESC;
