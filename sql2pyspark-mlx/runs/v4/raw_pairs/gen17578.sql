SELECT p.method, o.status, o.shipping_fee, c.country FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id LEFT OUTER JOIN customers c ON o.customer_id = c.id ORDER BY o.shipping_fee DESC;
