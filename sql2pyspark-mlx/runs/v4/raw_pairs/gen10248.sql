SELECT p.stock_qty, s.rating, s.country FROM products p RIGHT JOIN suppliers s ON s.id = p.supplier_id;
