SELECT p.id, p.category, p.cost, s.country FROM products p JOIN suppliers s ON p.supplier_id = s.id
