WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(status, '/', carrier) AS status_carrier,
        DAY(shipped_date) AS shipped_date_day,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM shipments
    WHERE order_id IS NOT NULL
      AND delivered_date >= '2025-05-11'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        status,
        carrier,
        cost
    FROM base
) src
PIVOT (
    MAX(cost)
    FOR carrier IN ('Flash' AS flash, 'FedEx' AS fedex)
)
