WITH remaining_keys AS (
    SELECT id, carrier
    FROM shipments
    INTERSECT ALL
    SELECT id, carrier
    FROM shipments
    WHERE YEAR(shipped_date) >= 2024
),
kept_rows AS (
    SELECT s.*
    FROM shipments s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.carrier = k.carrier
)
SELECT
    carrier,
    delivered_date,
    cost,
    status
FROM kept_rows
