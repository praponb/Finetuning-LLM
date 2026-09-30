WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(shipping_fee) OVER (PARTITION BY status ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date ASC, id ASC) AS row_seq,
        LAG(discount, 2, 0) OVER w_part AS prev_discount,
        CORR(shipping_fee, discount) OVER (PARTITION BY customer_id) AS corr_shipping_fee_discount,
        LAG(amount, 1) OVER w_part_2 AS prev_amount
    FROM orders
    WINDOW w_part AS (PARTITION BY status ORDER BY shipping_fee ASC, id ASC), w_part_2 AS (ORDER BY amount ASC, id ASC)
),
scored_orders AS (
    -- Window metrics over several different partitions
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
        cumulative_shipping_fee,
        row_seq,
        prev_discount,
        corr_shipping_fee_discount,
        prev_amount,
        SUM(row_seq) OVER (PARTITION BY channel ORDER BY corr_shipping_fee_discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        AVG(amount) OVER (PARTITION BY channel, status) AS channel_avg_amount
    FROM scored
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.payment_status
    FROM scored_orders s
    LEFT JOIN LATERAL (
            SELECT c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored_orders_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(amount, 2) OVER (PARTITION BY store_id ORDER BY row_seq ASC, id ASC) AS next_amount,
        NTILE(3) OVER (ORDER BY channel_avg_amount DESC, id ASC) AS channel_avg_amount_bucket
    FROM with_latest
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(corr_shipping_fee_discount) OVER w_hist AS payment_status_stddev_samp_corr_shipping_fee_discount,
        SUM(prev_discount) OVER (PARTITION BY status ORDER BY order_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_discount,
        AVG(corr_shipping_fee_discount) OVER (PARTITION BY channel ORDER BY shipping_fee ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS corr_shipping_fee_discount_avg_6
    FROM scored_orders_2
    WINDOW w_hist AS (PARTITION BY payment_status)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.product_id IN (1, 5, 33)
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.order_id = 36
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    COUNT(*) FILTER (WHERE status = 'CANCELLED') AS cancelled_rows,
    COUNT(*) FILTER (WHERE payment_status = 'REFUNDED') AS refunded_rows,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids
FROM not_flagged
GROUP BY 1;
