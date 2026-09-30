SELECT s.city, s.country, p.price
FROM suppliers s
JOIN products p ON p.supplier_id = s.id;
