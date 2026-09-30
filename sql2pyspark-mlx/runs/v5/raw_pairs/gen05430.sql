SELECT id, product_name, stock_qty
FROM products
WHERE stock_qty < 336
EXCEPT
SELECT id, product_name, stock_qty
FROM products
WHERE product_name IS NOT NULL
ORDER BY product_name;
