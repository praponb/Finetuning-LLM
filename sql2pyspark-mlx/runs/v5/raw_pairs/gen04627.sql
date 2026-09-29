WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        carrier,
        status,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC, id) AS seq_no,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        COUNT(cost) OVER w_grp AS status_count_cost,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY cost, id) AS cost_bucket,
        LEAD(cost, 1) OVER (ORDER BY delivered_date, id) AS next_cost
    FROM shipments
    WHERE cost BETWEEN 33 AND 72
    WINDOW w_grp AS (PARTITION BY status, order_id)
)
SELECT
    id,
    carrier,
    cost_share,
    status_count_cost,
    cost_bucket,
    CASE WHEN carrier RLIKE '^(UPS|Flash)$' THEN 'PRIORITY' WHEN carrier RLIKE '^(DHL|FedEx)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS carrier_zone
FROM scored
