WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY cost DESC NULLS FIRST) cost_cume_dist,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY cost DESC, id) cost_bucket
    FROM shipments
)
SELECT
    id,
    carrier,
    delivered_date,
    status,
    cost_cume_dist,
    cost_bucket,
    CASE WHEN carrier RLIKE '^(DHL|FedEx)$' THEN 'PRIORITY' WHEN carrier RLIKE '^(Kerry|Flash)$' THEN 'CORE' ELSE 'GROUP_A' END AS carrier_zone
FROM metrics
ORDER BY id DESC NULLS FIRST;
