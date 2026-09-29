SELECT o.id, o.shipping_fee
FROM orders o
WHERE o.discount < 0.38 AND o.id IN (SELECT order_id FROM shipments WHERE status != 'IN_TRANSIT')
ORDER BY o.shipping_fee DESC, id
LIMIT 3;
