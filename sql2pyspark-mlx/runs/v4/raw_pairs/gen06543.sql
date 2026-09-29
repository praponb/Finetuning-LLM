SELECT o.order_date, c.credit_limit, s.opened_date
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
RIGHT JOIN stores s ON o.store_id = s.id;
