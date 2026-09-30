WITH dim_totals AS (
    -- Join a dimension and aggregate
    SELECT
        dim.channel,
        dim.status,
        COUNT(*) AS num_rows,
        MAX(src.unit_price) AS max_unit_price
    FROM order_items src
    JOIN orders dim ON src.order_id = dim.id
    GROUP BY dim.channel, dim.status
)
SELECT
    channel,
    status,
    max_unit_price,
    CASE WHEN status RLIKE '^(CANCELLED|COMPLETED)$' THEN 'PRIORITY' WHEN status RLIKE '^(PENDING|SHIPPED)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS status_zone
FROM dim_totals
ORDER BY max_unit_price DESC NULLS FIRST, status;
