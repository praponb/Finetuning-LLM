SELECT
    id,
    order_id,
    shipped_date,
    carrier,
    status,
    cost,
    delivered_date,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS shipped_date_row,
    FIRST_VALUE(delivered_date) OVER (PARTITION BY carrier, status ORDER BY cost, id) AS first_delivered_date
FROM shipments
WHERE order_id = 27
