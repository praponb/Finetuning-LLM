SELECT o.id, o.customer_id, s.opened_date
FROM orders o
JOIN stores s ON o.store_id = s.id;
