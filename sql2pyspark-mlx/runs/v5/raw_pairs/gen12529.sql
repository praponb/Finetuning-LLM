SELECT o.id, o.amount, o.discount, c.country, c.city, c.is_active FROM orders o INNER JOIN customers c ON o.customer_id = c.id ORDER BY c.city, id DESC
