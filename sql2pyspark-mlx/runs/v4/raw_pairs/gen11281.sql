SELECT sale_date AS event_date
FROM store_sales
WHERE revenue < 1100
EXCEPT
SELECT launch_date AS event_date
FROM products;
