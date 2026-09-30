SELECT
    o.id,
    o.amount,
    o.store_id,
    o.discount
FROM orders o
WHERE o.customer_id IN (SELECT id FROM customers WHERE age <= 39);
