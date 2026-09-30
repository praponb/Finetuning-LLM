SELECT
    ss.store_id,
    ss.product_id,
    MIN(i.last_restock) first_last_restock,
    MIN(i.last_restock) min_last_restock
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (1, 2, 3, 7)
GROUP BY ss.store_id, ss.product_id;
