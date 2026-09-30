SELECT
    QUARTER(p.launch_date) AS launch_quarter,
    YEAR(p.launch_date) AS launch_year,
    AVG(oi.quantity) AS avg_quantity,
    COUNT(DISTINCT oi.quantity) AS unique_orders
FROM order_items oi
JOIN products p ON oi.product_id = p.id
RIGHT JOIN suppliers s ON p.supplier_id = s.id
WHERE s.country = 'JP'
GROUP BY QUARTER(p.launch_date), YEAR(p.launch_date)
ORDER BY launch_year, launch_quarter DESC
