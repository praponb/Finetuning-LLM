SELECT o.channel, c.age
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.channel IN ('partner', 'store') AND o.discount > 0.33
ORDER BY age DESC
