SELECT o.id, o.shipping_fee, c.city, s.store_name, s.opened_date FROM orders o INNER JOIN customers c ON c.id = o.customer_id INNER JOIN stores s ON o.store_id = s.id WHERE o.store_id IS NOT NULL;
