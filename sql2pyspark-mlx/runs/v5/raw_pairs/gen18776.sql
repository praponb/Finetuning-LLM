WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date < s.order_date
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_dense_rank,
        CORR(amount, discount) OVER (PARTITION BY status) AS corr_amount_discount,
        AVG(amount) OVER (PARTITION BY store_id ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior
    FROM eligible
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_windows s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date BETWEEN pr.starts_on AND pr.ends_on
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER w_running AS shipping_fee_dense_rank_bucket,
        LAST_VALUE(discount, TRUE) OVER (ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount
    FROM attributed
    WINDOW w_running AS (PARTITION BY channel ORDER BY shipping_fee_dense_rank DESC, id, promo_id)
)
SELECT
    id,
    customer_id,
    store_id,
    order_date,
    status,
    shipping_fee_dense_rank,
    corr_amount_discount,
    amount_avg_6_prior,
    promo_id,
    CASE WHEN status RLIKE '^(SHIPPED|COMPLETED)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS status_zone
FROM metrics;
