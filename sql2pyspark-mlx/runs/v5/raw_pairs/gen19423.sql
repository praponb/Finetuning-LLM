SELECT ss.store_id, MONTH(ss.sale_date) AS sale_month, ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (2, 4, 8, 9)
GROUP BY ss.store_id, MONTH(ss.sale_date)
ORDER BY sale_month DESC, avg_quantity;
