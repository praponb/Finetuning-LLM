WITH valid AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        discount,
        shipping_fee,
        LENGTH(status) AS status_length,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM orders
    WHERE order_date > DATE '2023-04-10'
)
SELECT
    status,
    COUNT(*) AS row_count,
    CORR(discount, shipping_fee) AS corr_discount_shipping_fee,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(shipping_fee) FILTER (WHERE status = 'SHIPPED') AS shipped_avg_shipping_fee,
    ROUND(AVG(discount), 2) AS avg_discount
FROM valid
GROUP BY status
HAVING avg_discount >= 0
ORDER BY corr_discount_shipping_fee DESC NULLS FIRST
