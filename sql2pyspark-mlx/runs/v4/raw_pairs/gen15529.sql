SELECT o.shipping_fee, p.paid_at FROM orders o LEFT OUTER JOIN payments p ON o.id = p.order_id ORDER BY p.paid_at DESC;
