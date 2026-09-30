SELECT s.id, s.country, COUNT(p.id) product_count
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id
GROUP BY s.id, s.country
ORDER BY product_count DESC, id, country
LIMIT 20;
