SELECT launch_date AS event_date, 'product' AS kind
FROM products
UNION
SELECT sale_date AS event_date, 'store_sale' AS kind
FROM store_sales
ORDER BY event_date;
