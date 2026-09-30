SELECT
    p.paid_at,
    p.order_id,
    o.order_date,
    o.discount,
    s.opened_date
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
INNER JOIN stores s ON o.store_id = s.id
