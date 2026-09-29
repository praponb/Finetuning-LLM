WITH filtered AS (
    SELECT *, CEIL(amount) AS amount_whole
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-05-03 23:00:00')
      AND order_id IS NOT NULL
),
metrics AS (
    SELECT
        *,
        MAX(amount_whole) OVER w_ordered AS last4_max_amount_whole,
        MIN(amount_whole) OVER w_ordered AS last4_min_amount_whole,
        SUM(amount_whole) OVER w_ordered AS last4_sum_amount_whole,
        SUM(amount_whole) OVER w_ordered_2 AS amount_whole_sum_90d
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_ordered_2 AS (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
),
metrics_payments AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        amount_whole_sum_90d,
        MIN(amount_whole) OVER (PARTITION BY status ORDER BY last4_min_amount_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_amount_whole,
        AVG(amount_whole) OVER (PARTITION BY status ORDER BY last4_min_amount_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_amount_whole,
        COUNT(amount_whole) OVER (PARTITION BY status ORDER BY last4_min_amount_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_amount_whole
    FROM metrics
),
kept_keys AS (
    SELECT order_id
    FROM metrics_payments
    INTERSECT
    SELECT order_id
    FROM payments
    WHERE TO_DATE(paid_at) < '2023-03-16'
),
reconciled AS (
    SELECT s.*
    FROM metrics_payments s
    INNER JOIN kept_keys k ON s.order_id = k.order_id
),
enriched AS (
    SELECT
        src.*,
        d.amount AS order_amount,
        d.channel
    FROM reconciled src
    LEFT JOIN orders d
        ON src.order_id <=> d.id
)
SELECT
    CASE WHEN amount_whole_sum_90d >= 316 THEN 'LARGE' ELSE 'SMALL' END AS amount_whole_sum_90d_tier,
    order_id,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count,
    COUNT(DISTINCT method) AS distinct_methods,
    COUNT(DISTINCT id) AS distinct_ids_2,
    GROUPING_ID() AS grouping_level
FROM enriched
GROUP BY GROUPING SETS ((CASE WHEN amount_whole_sum_90d >= 316 THEN 'LARGE' ELSE 'SMALL' END, order_id), (CASE WHEN amount_whole_sum_90d >= 316 THEN 'LARGE' ELSE 'SMALL' END), ());
