SELECT s.city, COUNT(*) cnt
FROM products p
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.city != 'Chiang Mai'
GROUP BY s.city
HAVING cnt > 4;
