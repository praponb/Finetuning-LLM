SELECT i.id, i.product_id, ss.sale_date
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE NOT (ss.store_id = 1 AND ss.store_id IN (3, 5));
