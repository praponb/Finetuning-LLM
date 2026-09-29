SELECT id, discount, customer_id FROM orders WHERE discount >= (SELECT MAX(discount) FROM orders WHERE order_date BETWEEN '2023-08-01' AND '2023-12-01');
