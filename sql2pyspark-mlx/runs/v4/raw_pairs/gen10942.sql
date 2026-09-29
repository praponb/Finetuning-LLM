SELECT o.id, o.amount, o.shipping_fee, c.age FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.is_active ORDER BY c.age DESC
