SELECT order_date AS event_date, 'order' AS record_type
FROM orders
WHERE order_date > '2023-09-26'
UNION ALL
SELECT sale_date AS event_date, 'store_sale' AS record_type
FROM store_sales
ORDER BY record_type
