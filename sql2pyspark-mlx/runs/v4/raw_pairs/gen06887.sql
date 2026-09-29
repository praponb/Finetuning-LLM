WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(order_date, 'yyyy-MM') AS order_date_label,
        discount + amount AS discount_plus_amount
    FROM orders
    WHERE channel IS NOT NULL
      AND order_date BETWEEN '2023-04-01' AND '2025-01-30'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(amount) OVER w_part AS channel_min_amount,
        CORR(discount, amount) OVER w_part AS corr_discount_amount,
        SUM(shipping_fee) OVER w_recent AS cumulative_shipping_fee,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY discount_plus_amount NULLS FIRST) AS discount_plus_amount_dense_rank,
        NTILE(10) OVER (PARTITION BY customer_id ORDER BY discount, id) AS discount_bucket
    FROM valid
    WINDOW w_part AS (PARTITION BY channel), w_recent AS (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.payment_amount
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_last e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.order_date >= pr.starts_on
       AND e.order_date < pr.ends_on + INTERVAL '14' DAY
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(shipping_fee, 2) OVER (PARTITION BY channel ORDER BY amount, id, promo_id) AS prev_shipping_fee,
        MIN(discount_bucket) OVER (PARTITION BY customer_id) AS customer_id_min_discount_bucket
    FROM with_window
),
with_windows_orders AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        order_date_label,
        discount_plus_amount,
        channel_min_amount,
        corr_discount_amount,
        cumulative_shipping_fee,
        discount_plus_amount_dense_rank,
        discount_bucket,
        payment_amount,
        promo_id,
        promo_code,
        discount_pct,
        prev_shipping_fee,
        customer_id_min_discount_bucket,
        RANK() OVER w_hist AS payment_amount_rank,
        CORR(amount, discount) OVER (PARTITION BY store_id) AS corr_amount_discount,
        SUM(promo_id) OVER (PARTITION BY store_id ORDER BY amount, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_promo_id
    FROM with_windows
    WINDOW w_hist AS (ORDER BY payment_amount DESC NULLS FIRST)
)
-- Multi-level aggregation
SELECT
    cumulative_promo_id,
    status,
    COUNT_IF(channel = 'partner') AS partner_events,
    AVG(cumulative_promo_id) FILTER (WHERE channel = 'web') AS web_avg_cumulative_promo_id,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    SUM(CASE WHEN channel = 'web' THEN payment_amount_rank ELSE 0 END) AS web_payment_amount_rank
FROM with_windows_orders
GROUP BY ROLLUP (cumulative_promo_id, status)
ORDER BY distinct_store_ids DESC NULLS FIRST
