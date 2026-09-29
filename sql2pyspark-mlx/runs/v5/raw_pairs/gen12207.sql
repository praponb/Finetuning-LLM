SELECT c.id, c.segment, COUNT(s.id) AS subscription_count
FROM customers c
LEFT OUTER JOIN subscriptions s ON s.customer_id = c.id
GROUP BY c.id, c.segment
ORDER BY subscription_count;
