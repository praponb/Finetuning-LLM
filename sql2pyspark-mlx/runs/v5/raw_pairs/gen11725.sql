SELECT CASE WHEN s.city IN ('Singapore', 'London') THEN 'tier_1' ELSE 'other' END AS city_group, SUM(i.quantity) AS total_quantity
FROM inventory i
JOIN stores s ON i.store_id = s.id
JOIN products p ON i.product_id = p.id
WHERE s.city IN ('Berlin', 'New York', 'Tokyo')
GROUP BY CASE WHEN s.city IN ('Singapore', 'London') THEN 'tier_1' ELSE 'other' END
ORDER BY total_quantity;
