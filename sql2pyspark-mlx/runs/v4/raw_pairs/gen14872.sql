SELECT o.id, o.customer_id, o.discount, o.order_date, s.store_name FROM orders o LEFT JOIN stores s ON o.store_id = s.id WHERE s.sqft < 10648 AND o.amount > 1060;
