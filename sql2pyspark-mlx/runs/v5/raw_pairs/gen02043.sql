SELECT
    shipments.id,
    shipments.shipped_date,
    shipments.delivered_date,
    shipments.status,
    orders.status AS order_status,
    orders.store_id,
    CONCAT(shipments.carrier, '/', shipments.status) AS carrier_status
FROM shipments
JOIN orders ON shipments.order_id = orders.id
ORDER BY shipments.id DESC, shipped_date DESC
