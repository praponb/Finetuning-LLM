SELECT c.segment, AVG(o.discount) AS avg_discount FROM orders o RIGHT JOIN customers c ON o.customer_id = c.id WHERE o.order_date >= '2024-12-01' GROUP BY c.segment;
