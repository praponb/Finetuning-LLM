SELECT s.city, COUNT(p.id) AS product_count
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
WHERE LOWER(s.supplier_name) LIKE '%aci%'
GROUP BY s.city
ORDER BY city ASC;
