SELECT s.city, COUNT(p.id) AS product_count, COUNT(CASE WHEN p.brand = 'Globex' THEN 1 END) AS globex_count
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
GROUP BY s.city
HAVING COUNT(p.id) >= 1
