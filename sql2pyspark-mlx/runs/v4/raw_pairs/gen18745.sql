SELECT o.id, o.discount, o.status, s.opened_date FROM orders o JOIN stores s ON o.store_id = s.id
