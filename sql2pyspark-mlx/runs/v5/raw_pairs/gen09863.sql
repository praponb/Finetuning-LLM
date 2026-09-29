SELECT o.id, o.status, o.shipping_fee, o.amount, s.region, s.opened_date, s.city FROM orders o INNER JOIN stores s ON s.id = o.store_id ORDER BY o.id DESC
