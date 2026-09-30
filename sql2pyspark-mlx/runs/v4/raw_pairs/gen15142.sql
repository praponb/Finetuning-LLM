WITH shipments_by_order AS (
    SELECT order_id, SUM(CASE WHEN status = 'IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit_count, COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END) AS fedex_count
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.status,
    o.discount,
    sbo.in_transit_count,
    sbo.fedex_count
FROM orders o
LEFT JOIN shipments_by_order sbo ON o.id = sbo.order_id
