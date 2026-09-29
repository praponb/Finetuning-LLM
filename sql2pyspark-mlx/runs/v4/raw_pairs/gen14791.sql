SELECT
    o.shipping_fee,
    o.discount,
    o.customer_id,
    oi.order_id,
    oi.unit_price
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE o.channel <> 'mobile' AND oi.unit_price <= 160;
