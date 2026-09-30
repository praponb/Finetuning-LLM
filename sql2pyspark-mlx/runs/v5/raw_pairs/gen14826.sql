SELECT i.quantity, ss.product_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE MONTH(ss.sale_date) IN (3, 7, 10) AND ss.product_id IN (1, 2, 5, 7)
ORDER BY ss.product_id DESC, i.quantity DESC
