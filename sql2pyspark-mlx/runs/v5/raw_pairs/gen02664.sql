SELECT
    c.id,
    c.country,
    COUNT(s.id) AS subscription_count,
    MAX(s.start_date) AS last_start_date,
    COUNT(CASE WHEN s.plan = 'enterprise' THEN 1 END) AS enterprise_count
FROM customers c
LEFT OUTER JOIN subscriptions s ON c.id = s.customer_id
GROUP BY c.id, c.country
ORDER BY enterprise_count DESC, subscription_count;
