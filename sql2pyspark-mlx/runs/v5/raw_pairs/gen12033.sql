WITH filtered AS (
    SELECT
        id,
        store_id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        GREATEST(shipping_fee, discount) AS greatest_shipping_fee_discount,
        CONCAT(channel, ' - ', status) AS channel_status
    FROM orders
    WHERE customer_id IS NULL
),
metrics AS (
    SELECT
        *,
        COUNT(discount) OVER w_recent AS running_count_discount,
        MAX(discount) OVER w_recent AS running_max_discount,
        SUM(discount) OVER w_recent AS running_sum_discount
    FROM filtered
    WINDOW w_recent AS (PARTITION BY status, store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.order_id IN (14, 20)
        )
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY greatest_shipping_fee_discount NULLS LAST, id) AS latest_rank
    FROM eligible
    QUALIFY latest_rank = 1
),
with_windows AS (
    SELECT
        id,
        status,
        channel,
        discount,
        shipping_fee,
        channel_status,
        running_count_discount,
        running_max_discount,
        running_sum_discount,
        latest_rank,
        AVG(running_count_discount) OVER (PARTITION BY store_id, status ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_count_discount_avg_6_prior,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY running_sum_discount DESC NULLS FIRST) AS running_sum_discount_cume_dist,
        NTILE(10) OVER (PARTITION BY status, store_id ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket
    FROM latest
    WHERE running_max_discount >= 0.15
)
SELECT
    channel,
    CASE WHEN running_sum_discount >= 1.94 THEN 'HIGH' ELSE 'LOW' END AS running_sum_discount_tier,
    CASE WHEN running_count_discount >= 17 THEN 'HIGH' ELSE 'LOW' END AS running_count_discount_tier,
    COUNT_IF(status = 'PENDING') AS pending_events,
    AVG(running_count_discount) FILTER (WHERE channel = 'partner') AS partner_avg_running_count_discount,
    APPROX_PERCENTILE(running_sum_discount, ARRAY(0.25, 0.5, 0.75)) AS running_sum_discount_quartiles,
    SIZE(COLLECT_SET(channel)) AS num_channels
FROM with_windows
GROUP BY channel, CASE WHEN running_sum_discount >= 1.94 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN running_count_discount >= 17 THEN 'HIGH' ELSE 'LOW' END
ORDER BY partner_avg_running_count_discount DESC NULLS FIRST
