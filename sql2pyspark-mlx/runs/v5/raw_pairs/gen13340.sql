SELECT c.id, c.name
FROM customers c
WHERE c.id IN (SELECT customer_id FROM orders WHERE channel = 'mobile')
ORDER BY c.id DESC, 2;
