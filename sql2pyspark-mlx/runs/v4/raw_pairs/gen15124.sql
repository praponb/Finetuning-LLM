SELECT p.id, p.stock_qty, p.brand, p.product_name, s.city, s.rating, s.country FROM products p LEFT JOIN suppliers s ON p.supplier_id = s.id WHERE s.city IN ('Berlin', 'Singapore', 'Bangkok')
