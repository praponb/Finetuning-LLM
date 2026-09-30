SELECT customer_id FROM orders WHERE status NOT IN ('CANCELLED', 'COMPLETED') UNION SELECT customer_id FROM orders WHERE amount BETWEEN 1010 AND 1570 ORDER BY customer_id
