SELECT
    ss.id,
    ss.product_id,
    s.region,
    s.city,
    p.category,
    DATE_TRUNC('MONTH', p.launch_date) AS launch_date_period
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
INNER JOIN products p ON ss.product_id = p.id
WHERE p.launch_date < '2025-10-01' AND s.opened_date BETWEEN '2023-03-27' AND '2023-09-09'
