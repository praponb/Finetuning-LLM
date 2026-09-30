WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        WEEKOFYEAR(order_date) order_date_weekofyear,
        COALESCE(channel, 'NONE') channel_filled
    FROM orders
    WHERE store_id IS NOT NULL
      AND amount > 810
),
base_orders AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        order_date_weekofyear,
        LEAST(shipping_fee, order_date_weekofyear) least_shipping_fee_order_date_weekofyear
    FROM base
    WHERE MONTH(order_date) IN (5, 8, 11)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered event_seq,
        NTILE(3) OVER (PARTITION BY store_id, channel ORDER BY discount DESC, id) discount_bucket,
        AVG(amount) OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) amount_avg_3,
        STDDEV_SAMP(discount) OVER w_grp store_id_stddev_samp_discount,
        LAG(order_date_weekofyear, 1) OVER (PARTITION BY store_id ORDER BY discount, id) prev_order_date_weekofyear
    FROM base_orders
    WINDOW w_ordered AS (PARTITION BY channel ORDER BY least_shipping_fee_order_date_weekofyear, id), w_grp AS (PARTITION BY store_id)
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.unit_price,
        latest.quantity
    FROM metrics s
    JOIN LATERAL (
            SELECT c.unit_price, c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    store_id,
    ROUND(SUM(order_date_weekofyear) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS order_date_weekofyear_per_web,
    SUM(CASE WHEN channel = 'web' THEN discount_bucket ELSE 0 END) AS web_discount_bucket
FROM with_last
GROUP BY ROLLUP (store_id)
ORDER BY order_date_weekofyear_per_web NULLS FIRST;
