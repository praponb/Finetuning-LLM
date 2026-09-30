SELECT DATE_TRUNC('MONTH', ss.sale_date) AS sale_month, MAX(i.last_restock) AS latest_last_restock
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (2, 3, 4, 5)
GROUP BY DATE_TRUNC('MONTH', ss.sale_date)
ORDER BY latest_last_restock DESC;
