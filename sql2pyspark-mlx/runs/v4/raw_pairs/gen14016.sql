WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, customer_id ORDER BY shipping_fee DESC NULLS FIRST, id) AS rank_in_group
    FROM orders
    QUALIFY rank_in_group = 1
)
SELECT
    store_id,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    channel,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS amount_per_store,
    COUNT(*) FILTER (WHERE status = 'CANCELLED') AS cancelled_rows,
    STDDEV_SAMP(discount) AS discount_stddev,
    SIZE(COLLECT_SET(channel)) AS num_channels
FROM deduped
GROUP BY GROUPING SETS ((store_id, DATE_TRUNC('WEEK', order_date), channel), (store_id, DATE_TRUNC('WEEK', order_date)))
ORDER BY channel DESC NULLS FIRST;
