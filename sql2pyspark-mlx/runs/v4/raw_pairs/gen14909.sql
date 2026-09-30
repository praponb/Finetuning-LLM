SELECT orders.id, orders.discount, orders.store_id
FROM orders
WHERE orders.customer_id IN (SELECT id FROM customers WHERE signup_date BETWEEN '2023-08-22' AND '2023-09-02');
