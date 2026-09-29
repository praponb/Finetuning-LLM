SELECT o.channel, c.name, c.segment
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
