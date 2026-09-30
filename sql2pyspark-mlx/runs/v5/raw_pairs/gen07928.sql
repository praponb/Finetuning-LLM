WITH supplier_stats AS (
    SELECT supplier_id, MIN(launch_date) AS first_launch_date
    FROM products
    GROUP BY supplier_id
)
SELECT s.supplier_name, s.country, ss.first_launch_date
FROM suppliers s
LEFT OUTER JOIN supplier_stats ss ON s.id = ss.supplier_id
