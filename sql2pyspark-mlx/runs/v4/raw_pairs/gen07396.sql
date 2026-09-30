SELECT o.store_id, o.discount, o.order_date, s.sqft, s.region FROM orders o INNER JOIN stores s ON o.store_id = s.id WHERE s.region = 'West'
