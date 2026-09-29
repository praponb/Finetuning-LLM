SELECT DATE_FORMAT(store_sales.sale_date, 'yyyy-MM') AS sale_month, ROUND(AVG(inventory.quantity), 2) AS avg_quantity, MIN(inventory.last_restock) AS min_last_restock
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
GROUP BY DATE_FORMAT(store_sales.sale_date, 'yyyy-MM')
ORDER BY avg_quantity DESC
