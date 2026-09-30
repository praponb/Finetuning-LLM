WITH filtered AS (
    SELECT
        *,
        ADD_MONTHS(delivered_date, 12) AS delivered_date_plus_months,
        COALESCE(carrier, 'OTHER') AS carrier_clean
    FROM shipments
    WHERE carrier IS DISTINCT FROM 'UPS'
      AND status = 'RETURNED'
)
SELECT carrier
FROM filtered
INTERSECT
SELECT carrier
FROM shipments
WHERE order_id = 25
