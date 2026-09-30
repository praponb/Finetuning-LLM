SELECT ss.product_id, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE MONTH(i.last_restock) IN (4, 5, 7)
GROUP BY ss.product_id
ORDER BY product_id DESC
