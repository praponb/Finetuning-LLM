SELECT
    customers.id,
    customers.age,
    customers.name,
    customers.credit_limit
FROM customers
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id AND orders.shipping_fee < 18);
