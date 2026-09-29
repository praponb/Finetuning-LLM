SELECT c.id, c.name, c.city
FROM customers c
WHERE c.id IN (SELECT customer_id FROM orders);
