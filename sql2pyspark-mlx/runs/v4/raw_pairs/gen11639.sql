SELECT
    s.delivered_date,
    s.shipped_date,
    o.discount,
    o.customer_id,
    o.status
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE s.status = 'IN_TRANSIT';
