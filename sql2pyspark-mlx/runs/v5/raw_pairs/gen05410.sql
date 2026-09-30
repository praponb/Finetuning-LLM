SELECT QUARTER(ss.sale_date) AS sale_quarter, ss.product_id, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY QUARTER(ss.sale_date), ss.product_id
ORDER BY sale_quarter DESC
