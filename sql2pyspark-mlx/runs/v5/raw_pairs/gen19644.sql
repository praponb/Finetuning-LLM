SELECT YEAR(ss.sale_date) AS sale_year, MAX(i.quantity) - MIN(i.quantity) AS quantity_range, MAX(i.last_restock) AS latest_last_restock
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY YEAR(ss.sale_date)
HAVING MAX(i.quantity) - MIN(i.quantity) >= 64
ORDER BY latest_last_restock, sale_year
LIMIT 20;
