SELECT p.stock_qty, p.brand, p.product_name, s.rating, s.city FROM products p INNER JOIN suppliers s ON p.supplier_id = s.id WHERE ARRAY_CONTAINS(p.tags, 'bestseller');
