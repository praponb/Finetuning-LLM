SELECT o.shipping_fee, o.discount, o.customer_id, p.status, p.paid_at FROM orders o LEFT JOIN payments p ON o.id = p.order_id WHERE p.status != 'FAILED' ORDER BY o.customer_id DESC
