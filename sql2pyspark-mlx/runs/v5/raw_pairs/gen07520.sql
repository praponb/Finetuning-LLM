SELECT i.product_id, ss.product_id AS store_sale_product_id, ss.store_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (1, 4) OR ss.revenue BETWEEN 2500 AND 3600
ORDER BY 3 DESC, ss.product_id DESC;
