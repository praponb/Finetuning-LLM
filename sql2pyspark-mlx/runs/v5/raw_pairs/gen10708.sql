SELECT
    c.id,
    c.segment,
    c.city,
    c.name
FROM customers c
WHERE c.id IN (SELECT customer_id FROM subscriptions WHERE status = 'ACTIVE');
