WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DAYOFWEEK(order_date) AS order_date_dayofweek,
        LOWER(channel) AS channel_lower
    FROM orders
    WHERE store_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount) OVER w_grp AS running_avg_amount,
        COUNT(amount) OVER w_grp AS running_count_amount,
        MIN(amount) OVER w_grp AS running_min_amount
    FROM cleaned
    WHERE order_date_dayofweek = 51
    WINDOW w_grp AS (PARTITION BY store_id, channel ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned_orders AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        channel_lower,
        running_avg_amount,
        running_count_amount,
        amount * running_count_amount AS amount_times_running_count_amount,
        CAST(running_min_amount AS INT) AS running_min_amount_int,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM scored
    WHERE order_date > DATE '2023-09-18'
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_item_unit_price
    FROM cleaned_orders s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Ranking and gap features
SELECT
    id,
    store_id,
    discount,
    shipping_fee,
    channel_lower,
    running_avg_amount,
    running_count_amount,
    running_min_amount_int,
    resolved_channel,
    order_item_unit_price,
    AVG(order_item_unit_price) OVER (PARTITION BY channel ORDER BY running_count_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS order_item_unit_price_avg_2,
    FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY channel ORDER BY amount_times_running_count_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
    MIN(running_min_amount_int) OVER w_grp AS store_id_min_running_min_amount_int
FROM latest_match
WINDOW w_grp AS (PARTITION BY store_id)
