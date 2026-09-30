SELECT c.is_active, c.segment, AVG(o.discount) AS avg_discount FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.amount < 1220 GROUP BY c.is_active, c.segment;
