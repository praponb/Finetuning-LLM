WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee, id) AS rn
    FROM orders
    QUALIFY rn = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(discount) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) discount_avg_2,
        LAG(amount, 1) OVER (PARTITION BY status ORDER BY order_date, id) prev_amount,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY discount DESC NULLS FIRST) discount_percent_rank
    FROM ranked
    WHERE rn <= 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(discount, 1, 0) OVER (PARTITION BY channel ORDER BY discount_avg_2, id) prev_discount,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY prev_amount DESC, id) row_seq,
        COUNT(amount) OVER (PARTITION BY customer_id, store_id) customer_id_count_amount
    FROM metrics
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_windows src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date < pr.ends_on + INTERVAL 14 DAYS
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', order_date) AS activity_month,
        CASE WHEN amount >= 1650 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
        PERCENTILE_APPROX(customer_id_count_amount, 0.95) AS customer_id_count_amount_p95,
        SUM(amount) AS total_amount,
        PERCENTILE_APPROX(promo_id, 0.95) AS promo_id_p95
    FROM with_window
    GROUP BY ROLLUP (DATE_TRUNC('MONTH', order_date), CASE WHEN amount >= 1650 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    activity_month,
    amount_tier,
    promo_id_p95
FROM stats
ORDER BY amount_tier DESC
