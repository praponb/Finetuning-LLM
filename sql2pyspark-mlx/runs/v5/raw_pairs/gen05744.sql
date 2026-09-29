SELECT
    c.city,
    c.segment,
    COUNT(CASE WHEN s.status = 'EXPIRED' THEN 1 END) AS expired_count,
    COUNT(s.monthly_fee) AS monthly_fee_count
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY c.city, c.segment;
