WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        UPPER(carrier) AS carrier_upper
    FROM shipments
    WHERE carrier IS NOT NULL
      AND shipped_date >= '2025-05-01'
)
SELECT order_id
FROM valid
EXCEPT
SELECT order_id
FROM shipments
WHERE order_id IS NULL
