SELECT o.id, o.customer_id, o.order_date, c.name, c.country, s.opened_date, s.sqft FROM orders o INNER JOIN customers c ON o.customer_id = c.id INNER JOIN stores s ON o.store_id = s.id;
