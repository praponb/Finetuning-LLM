SELECT price
FROM products
WHERE UPPER(product_name) LIKE '%APT%'
INTERSECT
SELECT revenue
FROM store_sales
WHERE product_id = 10;
