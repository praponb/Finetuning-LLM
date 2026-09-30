SELECT p.id, p.status, o.order_date, o.customer_id, s.store_name, s.city FROM payments p LEFT OUTER JOIN orders o ON o.id = p.order_id INNER JOIN stores s ON o.store_id = s.id ORDER BY 3;
