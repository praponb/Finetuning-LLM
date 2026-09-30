SELECT id, brand
FROM products
WHERE category IN ('Electronics', 'Grocery', 'Fashion')
UNION ALL
SELECT id, brand
FROM products
WHERE LOWER(product_name) LIKE '%ack%';
