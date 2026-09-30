SELECT
    CASE WHEN s.city IN ('New York', 'Bangkok') THEN 'core' ELSE 'other' END AS city_group,
    s.city,
    SUM(oi.quantity) AS total_quantity,
    AVG(oi.unit_price) AS avg_unit_price
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN stores s ON o.store_id = s.id
GROUP BY CASE WHEN s.city IN ('New York', 'Bangkok') THEN 'core' ELSE 'other' END, s.city
HAVING SUM(oi.quantity) > 15
ORDER BY avg_unit_price DESC;
