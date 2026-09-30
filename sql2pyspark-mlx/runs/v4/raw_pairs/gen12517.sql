SELECT
    s.region,
    s.city,
    COUNT(CASE WHEN o.status = 'PENDING' THEN 1 END) AS pending_count,
    MIN(o.amount) AS min_amount
FROM orders o
LEFT JOIN stores s ON o.store_id = s.id
WHERE o.customer_id IN (4, 7, 32)
GROUP BY s.region, s.city
