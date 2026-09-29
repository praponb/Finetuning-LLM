WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(shipping_fee) OVER (PARTITION BY status) status_count_shipping_fee,
        MAX(amount) OVER w_grp channel_max_amount
    FROM orders
    WINDOW w_grp AS (PARTITION BY channel)
)
-- Aggregate with conditional metrics
SELECT
    channel,
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed_rows,
    COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END) AS shipped_count,
    SUM(shipping_fee) AS total_shipping_fee,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SUM(status_count_shipping_fee) AS total_status_count_shipping_fee
FROM enriched
GROUP BY channel, quarter_start
HAVING COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END) < 1;
