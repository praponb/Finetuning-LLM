SELECT o.customer_id, o.order_date, p.amount, p.status FROM orders o INNER JOIN payments p ON p.order_id = o.id ORDER BY p.status DESC;
