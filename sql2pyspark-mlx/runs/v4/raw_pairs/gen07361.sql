SELECT s.region, AVG(ss.units) avg_units, COUNT(*) store_sale_count
FROM store_sales ss
INNER JOIN stores s ON s.id = ss.store_id
LEFT JOIN products p ON ss.product_id = p.id
WHERE s.store_name LIKE '%Town%'
GROUP BY s.region
ORDER BY store_sale_count DESC, avg_units ASC
