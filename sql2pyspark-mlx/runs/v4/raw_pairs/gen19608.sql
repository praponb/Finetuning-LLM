SELECT o.id, o.discount, o.order_date, o.status, s.store_name, s.region, s.opened_date FROM orders o INNER JOIN stores s ON o.store_id = s.id WHERE o.channel IN ('mobile', 'store')
