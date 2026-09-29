SELECT
    ss.product_id,
    ss.store_id,
    COUNT(*) AS row_count,
    MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.product_id, ss.store_id
ORDER BY store_id;
