SELECT c.id, c.name, o.id AS order_id FROM customers c INNER JOIN orders o ON c.id = o.customer_id;
