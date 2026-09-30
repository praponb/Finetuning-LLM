WITH with_windows AS (
    SELECT
        *,
        LAST_VALUE(discount, TRUE) OVER (PARTITION BY store_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount,
        LEAD(shipping_fee, 2) OVER (ORDER BY discount, id) AS next_shipping_fee,
        AVG(amount) OVER (ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
        ROW_NUMBER() OVER (ORDER BY discount, id) AS row_seq,
        ROUND(amount / SUM(amount) OVER (PARTITION BY channel), 4) AS amount_share
    FROM orders
)
SELECT
    CASE WHEN amount >= 1320 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    SIZE(COLLECT_SET(status)) AS num_status,
    ROUND(AVG(amount_share), 2) AS avg_amount_share,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS amount_per_mobile,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(amount_share) FILTER (WHERE channel = 'web') AS web_max_amount_share,
    STDDEV_SAMP(discount) AS discount_stddev,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN amount >= 1320 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SIZE(COLLECT_SET(status)) DESC) AS num_status_dense_rank
FROM with_windows
GROUP BY CASE WHEN amount >= 1320 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', order_date), DATE_TRUNC('MONTH', order_date)
