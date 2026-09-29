SELECT o.id, o.discount, o.amount, o.channel FROM orders o WHERE o.id IN (SELECT order_id FROM payments WHERE paid_at >= '2023-06-22') ORDER BY o.id DESC, 4;
