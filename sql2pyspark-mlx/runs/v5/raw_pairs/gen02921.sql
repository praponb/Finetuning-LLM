SELECT
    shipments.id,
    shipments.carrier,
    orders.amount,
    stores.region
FROM shipments
INNER JOIN orders ON shipments.order_id = orders.id
INNER JOIN stores ON stores.id = orders.store_id
WHERE stores.store_name LIKE 'Sia%' AND stores.sqft = 8377;
