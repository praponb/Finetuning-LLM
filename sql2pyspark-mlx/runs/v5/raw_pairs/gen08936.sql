SELECT delivered_date
FROM shipments
UNION ALL
SELECT sale_date AS delivered_date
FROM store_sales;
