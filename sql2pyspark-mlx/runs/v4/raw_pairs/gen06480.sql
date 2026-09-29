SELECT
    s.order_id,
    o.amount,
    o.order_date,
    o.discount
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE s.carrier IN ('FedEx', 'UPS', 'Kerry')
ORDER BY o.order_date DESC
