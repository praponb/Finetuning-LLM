SELECT DATE_TRUNC('MONTH', ss.sale_date) sale_month, ROUND(STDDEV(i.quantity), 2) stddev_quantity, COUNT(i.quantity) quantity_count
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY DATE_TRUNC('MONTH', ss.sale_date)
ORDER BY stddev_quantity DESC
