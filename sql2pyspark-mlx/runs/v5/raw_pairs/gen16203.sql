WITH recent_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name LIKE 'Pac%'
),
suppliers_stats AS (
    SELECT country, MIN(rating) AS min_rating
    FROM recent_suppliers
    GROUP BY country
)
SELECT *
FROM suppliers_stats
ORDER BY country DESC
