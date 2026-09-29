SELECT ss.product_id, COUNT(*) AS inventory_count, COUNT(i.quantity) AS quantity_count
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY ss.product_id
HAVING quantity_count > 4;
