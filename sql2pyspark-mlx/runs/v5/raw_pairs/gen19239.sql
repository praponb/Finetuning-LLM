SELECT s.city, p.cost
FROM suppliers s
LEFT OUTER JOIN products p ON p.supplier_id = s.id
WHERE s.rating BETWEEN 3.4 AND 4.1
