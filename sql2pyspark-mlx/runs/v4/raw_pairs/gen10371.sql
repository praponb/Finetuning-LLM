WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        shipped_date,
        cost,
        CONCAT_WS('-', carrier, status) AS carrier_status_key,
        CONCAT(status, ' - ', carrier) AS status_carrier,
        COALESCE(carrier, 'NONE') AS carrier_clean
    FROM shipments
    WHERE shipped_date >= DATE '2023-12-15'
      AND order_id IS NOT NULL
)
-- Attach orders attributes
SELECT e.*, b.amount
FROM filtered e
LEFT JOIN orders b
    ON e.order_id = b.id
