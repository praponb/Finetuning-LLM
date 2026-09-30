SELECT
    i.id,
    i.product_id,
    i.quantity,
    ss.sale_date
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue > 2400 AND ss.store_id IN (5, 6)
ORDER BY sale_date DESC
