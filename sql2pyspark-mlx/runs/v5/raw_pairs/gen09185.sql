SELECT
    oi.id,
    oi.unit_price,
    oi.product_id,
    o.shipping_fee,
    o.customer_id,
    s.opened_date,
    CONCAT_WS('/', o.channel, o.status) channel_status_key
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
RIGHT JOIN stores s ON o.store_id = s.id
WHERE s.sqft <= 12958;
