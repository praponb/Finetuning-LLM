SELECT
    id,
    carrier,
    shipped_date,
    ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS shipped_date_row
FROM shipments
WHERE carrier = 'DHL'
ORDER BY id DESC, shipped_date_row DESC
