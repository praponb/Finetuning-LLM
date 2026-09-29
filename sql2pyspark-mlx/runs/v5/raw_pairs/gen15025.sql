WITH supplier_summary AS (
    SELECT supplier_id, MAX(launch_date) AS last_launch_date, COUNT(price) AS price_count
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    ss.last_launch_date,
    ss.price_count
FROM suppliers s
LEFT JOIN supplier_summary ss ON s.id = ss.supplier_id
WHERE ss.price_count >= 6
ORDER BY country ASC;
