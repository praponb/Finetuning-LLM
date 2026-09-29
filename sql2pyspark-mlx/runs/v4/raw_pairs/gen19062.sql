SELECT i.store_id, i.last_restock, ss.store_id AS store_sale_store_id
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue BETWEEN 1900 AND 3800
ORDER BY ss.store_id DESC;
