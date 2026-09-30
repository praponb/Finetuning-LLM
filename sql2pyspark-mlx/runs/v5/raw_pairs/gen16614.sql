SELECT s.city, COUNT(*) AS row_count
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE LOWER(p.product_name) LIKE '%apt%'
GROUP BY s.city
ORDER BY row_count DESC, s.city DESC
LIMIT 50
