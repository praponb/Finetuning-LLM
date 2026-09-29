WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(cost, 1) AS cost_rounded,
        SUBSTRING(carrier, 1, 4) AS carrier_prefix
    FROM shipments
    WHERE delivered_date > DATE '2023-09-14'
      AND order_id IS NOT NULL
)
SELECT carrier
FROM valid
EXCEPT ALL
SELECT carrier
FROM shipments
WHERE YEAR(delivered_date) = 2023
