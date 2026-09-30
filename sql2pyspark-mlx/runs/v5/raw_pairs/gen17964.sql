WITH numbered AS (
    SELECT
        id,
        order_id,
        delivered_date,
        carrier,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) AS rnk
    FROM shipments
)
SELECT
    id,
    order_id,
    delivered_date,
    carrier
FROM numbered
WHERE rnk <= 2
ORDER BY id DESC
