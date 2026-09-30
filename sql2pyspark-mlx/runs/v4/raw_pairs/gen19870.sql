SELECT p.id, p.stock_qty, s.rating, s.country, COALESCE(s.rating, 0) AS rating_filled FROM products p JOIN suppliers s ON s.id = p.supplier_id WHERE s.country != 'US' ORDER BY id
