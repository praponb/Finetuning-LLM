SELECT
    CASE WHEN s.city IN ('Berlin', 'Chiang Mai') THEN 'tier_1' ELSE 'other' END AS city_group,
    s.country,
    SUM(i.quantity) AS total_quantity,
    ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.city IN ('Berlin', 'Chiang Mai') THEN 'tier_1' ELSE 'other' END, s.country
HAVING SUM(i.quantity) > 780
ORDER BY total_quantity DESC;
