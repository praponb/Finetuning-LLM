SELECT CASE WHEN s.rating >= 4.6 THEN 'high' WHEN s.rating >= 2.2 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(ss.units) AS total_units, MAX(ss.units) AS max_units
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
FULL OUTER JOIN suppliers s ON s.id = p.supplier_id
GROUP BY CASE WHEN s.rating >= 4.6 THEN 'high' WHEN s.rating >= 2.2 THEN 'mid' ELSE 'low' END
ORDER BY total_units DESC, rating_bucket
LIMIT 3;
