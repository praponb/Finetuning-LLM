SELECT s.country, COUNT(*) AS cnt
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.city IN ('Berlin', 'Bangkok')
GROUP BY s.country
ORDER BY cnt, country DESC;
