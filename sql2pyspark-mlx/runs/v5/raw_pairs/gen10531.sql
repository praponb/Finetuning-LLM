WITH by_dimension AS (
    -- Join a dimension and aggregate
    SELECT
        o.channel,
        src.status AS shipment_status,
        COUNT(*) AS num_rows,
        MAX(src.cost) AS max_cost,
        GROUPING_ID() AS grouping_level
    FROM shipments src
    INNER JOIN orders o ON src.order_id = o.id
    GROUP BY GROUPING SETS ((o.channel, src.status), (o.channel), ())
)
SELECT
    channel,
    shipment_status,
    num_rows,
    max_cost
FROM by_dimension
ORDER BY num_rows DESC;
