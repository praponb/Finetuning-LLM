SELECT s.country, p.supplier_id, p.cost FROM suppliers s LEFT JOIN products p ON s.id = p.supplier_id
