SELECT s.country, s.city, p.category, p.cost FROM suppliers s LEFT OUTER JOIN products p ON p.supplier_id = s.id WHERE s.supplier_name LIKE '%Euro%';
