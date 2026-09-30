SELECT products.id, products.category, products.product_name
FROM products
WHERE products.supplier_id IN (9, 38, 40) AND products.id IN (SELECT product_id FROM order_items WHERE order_id IN (10, 32))
ORDER BY id ASC;
