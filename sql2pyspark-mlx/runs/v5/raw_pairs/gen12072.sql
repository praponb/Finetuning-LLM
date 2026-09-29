WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name LIKE 'Eur%' AND rating < 2.9
),
suppliers_stats AS (
    SELECT city, SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) singapore_count, MAX(rating) max_rating
    FROM filtered_suppliers
    GROUP BY city
)
SELECT *
FROM suppliers_stats
WHERE max_rating >= 3.3;
