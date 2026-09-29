SELECT
    p.id,
    p.paid_at,
    o.discount,
    o.amount,
    s.opened_date,
    COALESCE(p.amount, 0) amount_filled
FROM payments p
JOIN orders o ON p.order_id = o.id
JOIN stores s ON o.store_id = s.id
WHERE s.city IN ('Singapore', 'Chiang Mai', 'New York')
ORDER BY id DESC
