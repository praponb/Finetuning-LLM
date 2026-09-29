SELECT p.method, p.paid_at, o.shipping_fee, o.store_id FROM payments p JOIN orders o ON p.order_id = o.id WHERE p.amount > 700 ORDER BY store_id DESC, shipping_fee;
