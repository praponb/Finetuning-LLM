WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(channel) AS channel_upper,
        COALESCE(amount, 0) AS amount_filled
    FROM orders
    WHERE status NOT IN ('CANCELLED', 'COMPLETED')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(status) AS status_upper,
        CASE WHEN discount >= 0.31 THEN 'High' WHEN discount >= 0.06 THEN 'Medium' ELSE 'Low' END AS discount_band,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM filtered
    WHERE order_date >= TO_DATE('2023-06-24')
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.order_item_unit_price,
        latest.quantity
    FROM base s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(discount) OVER (PARTITION BY store_id, customer_id ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_avg_6,
        AVG(quantity) OVER (PARTITION BY customer_id ORDER BY amount_filled, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_avg_5,
        NTILE(3) OVER w_grp AS amount_bucket,
        COUNT(amount) OVER (PARTITION BY channel) AS channel_count_amount
    FROM latest_match
    WINDOW w_grp AS (ORDER BY amount, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(channel_count_amount) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_channel_count_amount,
        COUNT(channel_count_amount) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_channel_count_amount,
        MIN(channel_count_amount) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_channel_count_amount,
        MAX(channel_count_amount) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_channel_count_amount
    FROM metrics
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.status,
        s.amount,
        s.discount,
        s.shipping_fee,
        s.channel_upper,
        s.amount_filled,
        s.resolved_channel,
        s.order_item_unit_price,
        s.quantity,
        s.discount_avg_6,
        s.amount_bucket,
        s.channel_count_amount,
        s.running_count_channel_count_amount,
        s.running_min_channel_count_amount,
        s.running_max_channel_count_amount,
        latest.payment_method,
        latest.paid_at
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.method AS payment_method, c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Running and moving aggregates
SELECT
    id,
    order_date,
    status,
    amount,
    shipping_fee,
    channel_upper,
    amount_filled,
    resolved_channel,
    quantity,
    running_count_channel_count_amount,
    running_min_channel_count_amount,
    payment_method,
    paid_at,
    DENSE_RANK() OVER (ORDER BY amount_filled NULLS LAST) AS amount_filled_dense_rank,
    LAST_VALUE(order_item_unit_price) IGNORE NULLS OVER (PARTITION BY payment_method ORDER BY discount_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_order_item_unit_price
FROM with_last
WHERE running_max_channel_count_amount BETWEEN 11 AND 16.1;
