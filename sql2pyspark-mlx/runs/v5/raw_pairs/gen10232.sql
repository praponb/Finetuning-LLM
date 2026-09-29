WITH base AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        CONCAT_WS('-', channel, status) AS channel_status_key
    FROM orders
    WHERE order_date > DATE '2023-05-14'
      AND store_id IS NOT NULL
),
scored AS (
    SELECT
        id,
        customer_id,
        order_date,
        status,
        shipping_fee,
        FIRST_VALUE(channel, TRUE) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        AVG(discount) OVER (PARTITION BY store_id, status ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS discount_avg_3
    FROM base
),
latest AS (
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY discount_avg_3, id) = 1
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY customer_id, status ORDER BY shipping_fee NULLS LAST) AS row_num
    FROM latest
    QUALIFY row_num = 1
),
deduped_orders AS (
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) = 1
),
with_latest AS (
    SELECT s.*, latest.shipped_date
    FROM deduped_orders s
    JOIN LATERAL (
            SELECT c.shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base_orders AS (
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM with_latest
    WHERE customer_id IS NOT NULL
),
daily AS (
    SELECT
        DATE_TRUNC('QUARTER', shipped_date) AS quarter_start,
        COUNT(*) AS row_count,
        SIZE(COLLECT_SET(status)) AS num_status,
        COUNT(DISTINCT status) AS distinct_status
    FROM base_orders
    GROUP BY CUBE (DATE_TRUNC('QUARTER', shipped_date))
)
SELECT
    quarter_start,
    row_count,
    num_status
FROM daily
ORDER BY num_status;
