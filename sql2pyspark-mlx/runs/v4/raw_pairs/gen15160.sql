SELECT o.order_date, p.method, p.amount FROM orders o LEFT JOIN payments p ON p.order_id = o.id
