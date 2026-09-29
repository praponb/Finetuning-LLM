WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(channel, 'N/A') AS channel_filled,
        shipping_fee + discount AS shipping_fee_plus_discount
    FROM orders
    WHERE order_date >= DATE '2023-07-19'
      AND order_date >= '2025-08-01'
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.customer_id,
        src.store_id,
        src.order_date,
        src.status,
        src.channel,
        src.amount,
        src.discount,
        src.shipping_fee,
        src.channel_filled,
        src.shipping_fee_plus_discount,
        s.sqft,
        s.store_name,
        s.opened_date
    FROM cleaned src
    INNER JOIN stores s
        ON src.store_id <=> s.id
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.order_date,
        s.status,
        s.discount,
        s.channel_filled,
        s.sqft,
        s.store_name,
        s.opened_date,
        latest.payment_amount
    FROM joined s
    LEFT JOIN LATERAL (
            SELECT c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        discount,
        payment_amount,
        RANK() OVER (PARTITION BY status, customer_id ORDER BY sqft DESC NULLS LAST) AS sqft_rank,
        FIRST_VALUE(payment_amount) IGNORE NULLS OVER (ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_payment_amount,
        NTILE(5) OVER (PARTITION BY status ORDER BY discount, id) AS discount_bucket,
        LEAD(sqft, 1) OVER (PARTITION BY status, store_id ORDER BY sqft, id) AS next_sqft
    FROM with_latest
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN first_known_payment_amount >= 1330 THEN 'LARGE' ELSE 'SMALL' END AS first_known_payment_amount_tier,
    MAX(next_sqft) AS max_next_sqft,
    ROUND(AVG(sqft_rank), 2) AS avg_sqft_rank,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(AVG(first_known_payment_amount), 2) AS avg_first_known_payment_amount
FROM with_windows
GROUP BY first_known_payment_amount_tier
HAVING avg_sqft_rank < 1000000
ORDER BY avg_first_known_payment_amount DESC NULLS LAST;
