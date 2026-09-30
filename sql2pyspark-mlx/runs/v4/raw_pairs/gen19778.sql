SELECT id, supplier_id, stock_qty, category, SUBSTRING(category, 1, 4) AS category_prefix FROM products;
