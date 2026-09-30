SELECT
    p.id,
    p.order_id,
    p.amount,
    p.method,
    o.shipping_fee,
    o.discount,
    CASE WHEN p.amount >= 1740 THEN 'High' WHEN p.amount >= 250 THEN 'Medium' ELSE 'Low' END AS amount_tier
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
ORDER BY amount_tier DESC;
