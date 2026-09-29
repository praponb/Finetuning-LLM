SELECT p.product_name, p.brand, ss.sale_date
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id
WHERE ss.revenue BETWEEN 1800 AND 2700 AND ss.revenue >= 3900
