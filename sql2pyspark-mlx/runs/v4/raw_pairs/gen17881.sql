SELECT p.brand, s.supplier_name
FROM products p
JOIN suppliers s ON s.id = p.supplier_id
WHERE p.category IN ('Grocery', 'Home', 'Electronics') AND p.price >= 680
ORDER BY supplier_name DESC
