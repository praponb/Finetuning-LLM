SELECT shipments.id, shipments.carrier
FROM shipments
WHERE shipments.delivered_date <= '2025-07-22' AND shipments.order_id IN (SELECT id FROM orders WHERE status != 'RETURNED')
ORDER BY id DESC;
