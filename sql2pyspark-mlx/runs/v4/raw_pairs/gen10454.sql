SELECT
    c.id,
    c.city,
    COUNT(s.id) AS subscription_count,
    COUNT(CASE WHEN s.status = 'CANCELLED' THEN 1 END) AS cancelled_count
FROM customers c
LEFT JOIN subscriptions s ON s.customer_id = c.id
WHERE s.plan IN ('standard', 'premium')
GROUP BY c.id, c.city
HAVING COUNT(CASE WHEN s.status = 'CANCELLED' THEN 1 END) > 5
ORDER BY city
