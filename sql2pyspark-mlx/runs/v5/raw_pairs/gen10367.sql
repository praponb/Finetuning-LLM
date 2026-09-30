SELECT revenue
FROM store_sales
WHERE sale_date >= '2023-05-22'
INTERSECT
SELECT cost
FROM products
