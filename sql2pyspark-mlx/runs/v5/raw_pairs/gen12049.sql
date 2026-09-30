SELECT
    p.id,
    p.order_id,
    o.order_date,
    o.shipping_fee,
    o.channel,
    COALESCE(p.amount, 0) AS amount_filled
FROM payments p
LEFT JOIN orders o ON o.id = p.order_id
WHERE o.store_id IN (6, 26, 31)
ORDER BY channel ASC;
