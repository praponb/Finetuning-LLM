SELECT
    p.id,
    p.amount,
    o.amount AS order_amount,
    o.store_id,
    s.store_name
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
LEFT OUTER JOIN stores s ON o.store_id = s.id
WHERE o.channel IN ('partner', 'store', 'mobile') OR p.paid_at < '2025-06-01'
ORDER BY order_amount DESC
