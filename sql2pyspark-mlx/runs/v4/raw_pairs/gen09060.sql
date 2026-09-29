SELECT
    p.id,
    p.cost,
    p.product_name,
    p.launch_date
FROM products p
WHERE p.stock_qty BETWEEN 356 AND 376 AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.store_id = 5)
ORDER BY product_name
