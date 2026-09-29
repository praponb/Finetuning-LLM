SELECT YEAR(ss.sale_date) AS sale_year, MIN(i.last_restock) AS first_last_restock, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY YEAR(ss.sale_date)
