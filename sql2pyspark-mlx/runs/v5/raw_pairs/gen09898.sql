SELECT i.store_id, i.product_id, ss.product_id AS store_sale_product_id
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (3, 6, 8, 10) AND ss.product_id IN (4, 5, 6, 7)
