SELECT
    p.id,
    p.order_id,
    o.channel,
    LEAST(o.amount, o.shipping_fee) AS least_amount_shipping_fee
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
ORDER BY channel DESC
