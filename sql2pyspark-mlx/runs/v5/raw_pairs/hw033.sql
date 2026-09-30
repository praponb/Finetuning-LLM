SELECT c.id, c.name, o.order_date FROM customers c LEFT JOIN orders o ON c.id = o.customer_id;
