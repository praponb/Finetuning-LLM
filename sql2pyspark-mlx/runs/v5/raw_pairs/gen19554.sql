SELECT o.id, o.customer_id, o.store_id, o.amount, s.region, s.city FROM orders o INNER JOIN stores s ON o.store_id = s.id
