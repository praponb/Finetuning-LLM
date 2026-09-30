WITH filtered AS (
    SELECT *, DATE_TRUNC('month', paid_at) AS paid_at_period
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-12-18 06:00:00')
),
flagged AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
enriched AS (
    SELECT
        s.*,
        d.shipping_fee,
        d.order_date,
        d.channel
    FROM sessionized s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
filtered_payments AS (
    SELECT *, MONTH(order_date) AS order_date_month
    FROM enriched
    WHERE HOUR(paid_at) BETWEEN 7 AND 11
),
enriched_payments AS (
    SELECT
        *,
        AVG(is_new_session) OVER (ORDER BY CAST(paid_at_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS is_new_session_avg_30d,
        AVG(order_date_month) OVER (PARTITION BY channel, order_id) AS channel_avg_order_date_month,
        MIN(shipping_fee) OVER (PARTITION BY status) AS status_min_shipping_fee,
        SUM(is_new_session) OVER (PARTITION BY status ORDER BY CAST(paid_at_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS is_new_session_sum_30d,
        LAG(session_id, 1, 0) OVER (PARTITION BY order_id, status ORDER BY session_id, id) AS prev_session_id
    FROM filtered_payments
),
with_prev AS (
    SELECT *, LAG(paid_at_period) OVER (PARTITION BY channel ORDER BY paid_at_period, id) AS prev_paid_at_period
    FROM enriched_payments
),
gaps AS (
    SELECT *, CASE WHEN prev_paid_at_period IS NULL OR paid_at_period > prev_paid_at_period + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session_2
    FROM with_prev
),
sessionized_payments AS (
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY channel ORDER BY paid_at_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY prev_paid_at_period NULLS LAST, id) AS dedup_rank
    FROM sessionized_payments
    QUALIFY dedup_rank = 1
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.status,
    s.is_new_session,
    s.session_id,
    s.shipping_fee,
    (SELECT MAX(t2.amount) FROM payments t2 WHERE t2.order_id = s.order_id) AS max_payment_amount
FROM ranked s
