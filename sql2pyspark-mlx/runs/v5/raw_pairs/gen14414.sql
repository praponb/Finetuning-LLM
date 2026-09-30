WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(shipping_fee, discount) AS greatest_shipping_fee_discount
    FROM orders
    WHERE order_date > DATE '2023-04-05'
      AND amount BETWEEN 1050 AND 1110
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.channel,
        s.discount,
        s.shipping_fee,
        top1.payment_status,
        top1.method
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.status AS payment_status, c.method
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_latest e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.order_date BETWEEN pr.starts_on AND pr.ends_on
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_window s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND TO_DATE(c.paid_at) >= '2025-01-01'
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_date,
        discount,
        shipping_fee,
        promo_id,
        discount_pct,
        LAG(discount_pct) OVER w_hist AS prev_discount_pct,
        LEAD(promo_id) OVER w_hist AS next_promo_id,
        LAG(promo_id) OVER w_hist AS prev_promo_id,
        ROW_NUMBER() OVER w_hist AS event_seq,
        LAG(promo_id, 1) OVER (ORDER BY shipping_fee, id, promo_id) AS prev_promo_id_2
    FROM eligible
    WINDOW w_hist AS (PARTITION BY customer_id, method ORDER BY promo_id, id, promo_id)
),
eligible_orders AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.order_id IN (7, 10, 20, 23)
        )
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    promo_id,
    COUNT(*) AS num_rows,
    ROUND(AVG(next_promo_id), 2) AS avg_next_promo_id,
    AVG(shipping_fee) AS avg_shipping_fee,
    APPROX_PERCENTILE(promo_id, 0.95) AS promo_id_p95,
    ROUND(AVG(promo_id), 2) AS avg_promo_id
FROM eligible_orders
GROUP BY 1, 2;
