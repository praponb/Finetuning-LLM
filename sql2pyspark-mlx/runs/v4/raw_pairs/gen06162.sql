SELECT p.category, i.product_id
FROM products p
JOIN inventory i ON i.product_id = p.id
WHERE p.stock_qty BETWEEN 102 AND 258
