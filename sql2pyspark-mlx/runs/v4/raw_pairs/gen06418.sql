WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(status) AS status_lower,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM shipments
    WHERE carrier IS NOT NULL
)
SELECT status, order_id
FROM valid
INTERSECT ALL
SELECT status, order_id
FROM shipments
WHERE order_id IS NOT NULL;
