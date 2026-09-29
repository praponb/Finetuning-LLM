SELECT o.id, o.shipping_fee, o.status, s.store_name, s.city FROM orders o INNER JOIN stores s ON o.store_id = s.id ORDER BY city;
