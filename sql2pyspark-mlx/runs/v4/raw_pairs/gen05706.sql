SELECT
    s.opened_date,
    s.region,
    o.shipping_fee,
    o.order_date
FROM stores s
LEFT JOIN orders o ON o.store_id = s.id
WHERE o.order_date > '2025-04-01'
ORDER BY s.opened_date
