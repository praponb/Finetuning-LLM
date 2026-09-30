WITH dim_totals AS (
    -- Join a dimension and aggregate
    SELECT
        p.status AS order_status,
        p.channel,
        s.status AS payment_status,
        COUNT(*) AS num_rows,
        MAX(s.amount) AS max_amount
    FROM payments s
    LEFT JOIN orders p ON s.order_id = p.id
    GROUP BY GROUPING SETS ((p.status, p.channel, s.status), (p.status, p.channel), (p.status))
)
SELECT
    order_status,
    channel,
    payment_status,
    num_rows,
    max_amount,
    CASE WHEN channel RLIKE '^(partner|mobile)$' THEN 'GROUP_A' WHEN channel RLIKE '^(web|store)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS channel_zone
FROM dim_totals
ORDER BY channel_zone NULLS FIRST
