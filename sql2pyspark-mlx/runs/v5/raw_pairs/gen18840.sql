SELECT id, product_name
FROM products
WHERE LENGTH(product_name) > 10
UNION
SELECT id, product_name
FROM products
WHERE stock_qty > 207
