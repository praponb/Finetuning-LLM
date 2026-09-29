SELECT
    s.id,
    s.city,
    COUNT(ss.id) AS store_sale_count,
    MIN(ss.units) AS min_units,
    MIN(ss.revenue) AS min_revenue
FROM stores s
INNER JOIN store_sales ss ON s.id = ss.store_id
GROUP BY s.id, s.city
HAVING MIN(ss.units) > 11
ORDER BY min_units, id, city
LIMIT 50;
