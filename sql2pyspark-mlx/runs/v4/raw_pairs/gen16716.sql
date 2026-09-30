SELECT products.id, products.stock_qty, products.product_name, products.category FROM products WHERE products.supplier_id NOT IN (SELECT id FROM suppliers WHERE supplier_name LIKE '%Lion%');
