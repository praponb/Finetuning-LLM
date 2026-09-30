SELECT o.id, o.order_date, s.region, s.opened_date, c.email, c.age FROM orders o INNER JOIN stores s ON o.store_id = s.id INNER JOIN customers c ON c.id = o.customer_id WHERE s.sqft <> 15380;
