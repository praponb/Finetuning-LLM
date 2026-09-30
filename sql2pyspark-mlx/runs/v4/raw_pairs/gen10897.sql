SELECT
    ss.id,
    ss.sale_date,
    ss.revenue,
    ss.store_id
FROM store_sales ss
WHERE ss.product_id NOT IN (SELECT id FROM products WHERE launch_date < '2024-11-01')
