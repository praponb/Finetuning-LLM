SELECT s.id, s.supplier_name, s.country, x.category FROM suppliers s LEFT JOIN LATERAL (SELECT c.category FROM products c WHERE c.supplier_id = s.id ORDER BY c.cost DESC, c.id DESC LIMIT 1) x ON TRUE
