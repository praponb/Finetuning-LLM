WITH with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.payment_amount,
        x.payment_paid_at
    FROM orders s
    JOIN LATERAL (
            SELECT c.amount AS payment_amount, c.paid_at AS payment_paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_latest src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.payment_paid_at >= pr.starts_on
       AND src.payment_paid_at <= DATE_ADD(pr.ends_on, 3)
),
latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_id,
        s.order_date,
        s.status,
        s.channel,
        s.discount,
        s.shipping_fee,
        s.payment_paid_at,
        s.promo_id,
        s.promo_code,
        top1.delivered_date,
        top1.carrier
    FROM matched s
    LEFT JOIN LATERAL (
            SELECT c.delivered_date, c.carrier
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.delivered_date <= s.payment_paid_at
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(promo_id) OVER (PARTITION BY store_id ORDER BY promo_id, id, promo_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS promo_id_avg_6_prior,
        LAST_VALUE(promo_id) IGNORE NULLS OVER (PARTITION BY carrier ORDER BY payment_paid_at, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_promo_id
    FROM latest_match
    WHERE TO_DATE(payment_paid_at) >= '2024-03-01'
)
-- Grouped metrics
SELECT
    store_id,
    DATE_TRUNC('QUARTER', payment_paid_at) AS quarter_start,
    COUNT(*) AS cnt,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed_count,
    MAX(payment_paid_at) AS last_payment_paid_at
FROM enriched
GROUP BY GROUPING SETS ((store_id, DATE_TRUNC('QUARTER', payment_paid_at)), (store_id), ())
ORDER BY cnt DESC
