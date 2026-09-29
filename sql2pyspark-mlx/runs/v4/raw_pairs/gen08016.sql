SELECT o.store_id, o.order_date, o.amount, p.paid_at FROM orders o LEFT JOIN payments p ON o.id = p.order_id WHERE p.status != 'FAILED' ORDER BY amount DESC, p.paid_at ASC
