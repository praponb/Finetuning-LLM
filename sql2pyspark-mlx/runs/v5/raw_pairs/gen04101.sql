SELECT
    orders.amount,
    orders.status,
    orders.channel,
    shipments.delivered_date,
    shipments.status AS shipment_status
FROM orders
LEFT OUTER JOIN shipments ON shipments.order_id = orders.id
WHERE shipments.carrier IN ('Flash', 'Kerry', 'DHL')
ORDER BY shipments.delivered_date, orders.status
