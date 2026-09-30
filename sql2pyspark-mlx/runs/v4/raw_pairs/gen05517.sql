SELECT s.region, CASE WHEN s.region IN ('East', 'North') THEN 'core' ELSE 'other' END AS region_group, ROUND(AVG(p.amount), 2) AS avg_amount
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
JOIN stores s ON o.store_id = s.id
GROUP BY s.region, CASE WHEN s.region IN ('East', 'North') THEN 'core' ELSE 'other' END;
