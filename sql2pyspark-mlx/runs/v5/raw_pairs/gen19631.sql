SELECT orders.id, orders.discount, orders.amount
FROM orders
WHERE orders.amount >= 1250 AND orders.customer_id IN (SELECT id FROM customers WHERE NOT is_active)
ORDER BY id DESC, discount;
