WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY amount DESC) AS amount_rank,
        CORR(amount, discount) OVER (PARTITION BY status, store_id) AS corr_amount_discount,
        AVG(amount) OVER (PARTITION BY store_id) AS store_id_avg_amount
    FROM orders
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at <= s.order_date
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(channel, ' - ', status) AS channel_status,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM eligible
    WHERE order_date > TO_DATE('2024-02-08')
      AND store_id_avg_amount = 1373
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(store_id_avg_amount / SUM(store_id_avg_amount) OVER (PARTITION BY store_id), 4) AS store_id_avg_amount_share,
        ROW_NUMBER() OVER (PARTITION BY customer_id, channel ORDER BY amount_rank, id) AS event_seq,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id) AS shipping_fee_bucket
    FROM base
),
eligible_orders AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.unit_price BETWEEN 110 AND 240
        )
),
enriched_orders AS (
    -- Running and moving aggregates
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
        amount_rank,
        corr_amount_discount,
        store_id_avg_amount,
        channel_status,
        channel_filled,
        store_id_avg_amount_share,
        event_seq,
        shipping_fee_bucket,
        COUNT(discount) OVER (PARTITION BY status) AS status_count_discount,
        LEAD(event_seq, 1, 0) OVER (ORDER BY store_id_avg_amount_share, id) AS next_event_seq
    FROM eligible_orders
),
enriched_orders_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY status ORDER BY amount DESC, id) AS amount_bucket,
        FIRST_VALUE(status_count_discount) IGNORE NULLS OVER (PARTITION BY store_id, channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_count_discount
    FROM enriched_orders
)
-- Multi-level aggregation
SELECT
    customer_id,
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
    AVG(status_count_discount) FILTER (WHERE status = 'PENDING') AS pending_avg_status_count_discount,
    MAX(next_event_seq) FILTER (WHERE channel = 'store') AS store_max_next_event_seq,
    ROUND(AVG(corr_amount_discount), 2) AS avg_corr_amount_discount,
    MAX(shipping_fee_bucket) FILTER (WHERE status = 'SHIPPED') AS shipped_max_shipping_fee_bucket,
    AVG(status_count_discount) AS avg_status_count_discount
FROM enriched_orders_2
GROUP BY customer_id, DATE_TRUNC('MONTH', order_date)
