SELECT
    i.id,
    i.quantity,
    i.store_id,
    i.last_restock,
    ss.sale_date
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue BETWEEN 1100 AND 2000
ORDER BY i.id DESC
