SELECT
    shipped_date,
    carrier,
    order_id,
    CAST(cost AS INT) cost_int,
    CONCAT(status, ' ', carrier) status_carrier
FROM shipments
WHERE order_id IS NOT NULL AND carrier = 'DHL' AND delivered_date >= '2023-05-01'
