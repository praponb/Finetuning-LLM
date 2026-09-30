SELECT
    p.id,
    p.paid_at,
    p.status,
    p.order_id,
    o.channel,
    o.amount,
    o.discount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
WHERE o.amount >= 820 AND o.discount >= 0.14
ORDER BY 3, id
LIMIT 3;
