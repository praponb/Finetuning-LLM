SELECT
    ss.sale_date,
    p.product_name,
    p.brand,
    s.opened_date,
    s.region
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
RIGHT JOIN stores s ON s.id = ss.store_id
WHERE s.city IN ('Berlin', 'Chiang Mai') AND ss.revenue BETWEEN 1500 AND 3300
ORDER BY sale_date DESC
