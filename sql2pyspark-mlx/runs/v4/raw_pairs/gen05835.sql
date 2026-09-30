SELECT price
FROM products
UNION ALL
SELECT revenue AS price
FROM store_sales;
