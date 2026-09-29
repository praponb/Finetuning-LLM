SELECT c.id, c.name, COUNT(s.id) AS subscription_count
FROM customers c
LEFT JOIN subscriptions s ON c.id = s.customer_id
GROUP BY c.id, c.name
ORDER BY subscription_count
