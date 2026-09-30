SELECT p.id, p.method, p.order_id, o.channel, o.discount FROM payments p JOIN orders o ON p.order_id = o.id;
