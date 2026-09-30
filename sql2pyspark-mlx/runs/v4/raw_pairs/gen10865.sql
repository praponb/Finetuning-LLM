SELECT o.id, o.order_date, s.opened_date FROM orders o RIGHT OUTER JOIN stores s ON s.id = o.store_id ORDER BY id DESC;
