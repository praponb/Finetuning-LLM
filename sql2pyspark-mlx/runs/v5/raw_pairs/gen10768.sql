SELECT
    ss.store_id,
    ss.product_id,
    SUM(i.quantity) total_quantity,
    AVG(i.quantity) avg_quantity
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.store_id, ss.product_id
ORDER BY product_id DESC;
