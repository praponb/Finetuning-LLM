WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(status, ' - ', carrier) AS status_carrier,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-04-29')
      AND cost >= 79
)
-- Attach orders attributes
SELECT
    s.*,
    b.order_date,
    b.discount
FROM filtered s
INNER JOIN orders b
    ON s.order_id = b.id
WHERE b.store_id IS NOT NULL
