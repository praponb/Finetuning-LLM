SELECT
    i.id,
    i.last_restock,
    ss.sale_date,
    ss.product_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue < 2500 AND ss.revenue < 1100
ORDER BY i.last_restock;
