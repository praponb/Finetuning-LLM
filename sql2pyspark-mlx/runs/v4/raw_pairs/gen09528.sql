SELECT
    c.id,
    c.email,
    c.is_active,
    c.city
FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
