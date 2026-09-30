SELECT
    carrier,
    shipped_date,
    order_id,
    SUBSTRING(carrier, 1, 4) AS carrier_prefix
FROM shipments
WHERE carrier NOT IN ('FedEx', 'UPS', 'Flash');
