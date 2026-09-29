SELECT c.country, COUNT(s.id) subscription_count
FROM customers c
LEFT OUTER JOIN subscriptions s ON s.customer_id = c.id
WHERE s.plan IN ('basic', 'enterprise', 'premium')
GROUP BY c.country
ORDER BY country DESC
