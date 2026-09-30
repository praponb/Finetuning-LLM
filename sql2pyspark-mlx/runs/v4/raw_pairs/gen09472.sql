SELECT
    o.id,
    o.channel,
    o.shipping_fee,
    o.status
FROM orders o
WHERE o.customer_id IN (4, 14, 23) AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.unit_price > 250);
