SELECT s.id, COUNT(p.id) AS product_count
FROM suppliers s
LEFT OUTER JOIN products p ON s.id = p.supplier_id
WHERE s.city NOT IN ('Bangkok', 'London', 'Tokyo')
GROUP BY s.id
HAVING COUNT(p.id) > 2
ORDER BY id DESC;
