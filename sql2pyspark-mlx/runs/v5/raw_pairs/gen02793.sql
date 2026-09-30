WITH top_shipments AS (
    SELECT
        id,
        carrier,
        cost,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS rn
    FROM shipments
    WHERE delivered_date > '2024-12-01'
)
SELECT
    id,
    carrier,
    cost,
    order_id
FROM top_shipments
WHERE rn <= 3;
