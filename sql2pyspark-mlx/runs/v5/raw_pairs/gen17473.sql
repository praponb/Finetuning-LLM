SELECT p.id, p.price, p.product_name
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id)
ORDER BY price DESC, product_name DESC, id
LIMIT 5;
