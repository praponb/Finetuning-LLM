WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(status, 1, 3) AS status_prefix,
        COALESCE(carrier, 'UNKNOWN') AS carrier_filled
    FROM shipments
    WHERE delivered_date > DATE '2023-07-30'
      AND carrier IS NOT NULL
)
-- Attach orders attributes
SELECT
    e.id,
    e.order_id,
    e.carrier,
    e.delivered_date,
    e.cost,
    e.carrier_filled,
    o.status AS order_status
FROM filtered e
LEFT JOIN orders o
    ON e.order_id = o.id
WHERE o.amount < 600;
