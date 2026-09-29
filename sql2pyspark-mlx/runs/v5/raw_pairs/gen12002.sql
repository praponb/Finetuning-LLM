SELECT p.launch_date, ss.units, ss.revenue
FROM products p
LEFT OUTER JOIN store_sales ss ON p.id = ss.product_id
WHERE ss.revenue IS NULL AND ss.product_id IN (2, 4, 7, 10)
ORDER BY ss.revenue DESC;
