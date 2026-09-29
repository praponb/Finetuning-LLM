SELECT p.id, p.supplier_id
FROM products p
WHERE p.price BETWEEN 890 AND 1130 AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.units != 32)
ORDER BY id
