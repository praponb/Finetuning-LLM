WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY store_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        NTILE(4) OVER (PARTITION BY channel, status ORDER BY amount DESC, id) AS amount_bucket,
        FIRST_VALUE(channel, TRUE) OVER (ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        SUM(discount) OVER (PARTITION BY channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount,
        AVG(shipping_fee) OVER (PARTITION BY store_id, status ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_5
    FROM orders
)
-- Grouped metrics
SELECT
    CASE WHEN cumulative_discount >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_discount_tier,
    customer_id,
    status,
    ROUND(SUM(amount_bucket) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS amount_bucket_per_mobile,
    ROUND(AVG(running_amount), 2) AS avg_running_amount,
    COUNT_IF(status = 'CANCELLED') AS cancelled_events,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count
FROM enriched
GROUP BY CASE WHEN cumulative_discount >= 2 THEN 'PREMIUM' ELSE 'STANDARD' END, customer_id, status
