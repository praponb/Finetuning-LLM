SELECT
    id,
    carrier,
    order_id,
    delivered_date,
    status,
    shipped_date,
    ROW_NUMBER() OVER (PARTITION BY carrier, order_id ORDER BY delivered_date DESC, id) AS rn,
    NTILE(10) OVER (PARTITION BY status ORDER BY shipped_date, id) AS bucket
FROM shipments
WHERE carrier = 'Flash';
