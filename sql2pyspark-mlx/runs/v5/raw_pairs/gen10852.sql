SELECT p.price, p.product_name, s.rating
FROM products p
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.price < 1060
ORDER BY p.price DESC;
