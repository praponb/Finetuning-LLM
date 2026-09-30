WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        amount * discount AS amount_times_discount,
        CAST(shipping_fee AS INT) AS shipping_fee_int,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM orders
    WHERE channel IS DISTINCT FROM 'web'
      AND channel IN ('mobile', 'store')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        amount_times_discount,
        shipping_fee_int,
        resolved_channel,
        CAST(amount_times_discount AS INT) AS amount_times_discount_int,
        ROUND(amount_times_discount * 1.07, 2) AS amount_times_discount_scaled
    FROM cleaned
    WHERE shipping_fee <= 11
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.name,
        c.age
    FROM filtered src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.payment_method,
        latest.paid_at
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.method AS payment_method, c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_last
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY shipping_fee NULLS LAST) = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM ranked a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.order_date >= sub.start_date
       AND a.order_date <= DATE_ADD(sub.end_date, 7)
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.amount,
        s.discount,
        s.shipping_fee,
        s.amount_times_discount,
        s.shipping_fee_int,
        s.resolved_channel,
        s.amount_times_discount_scaled,
        s.name,
        s.subscription_id,
        s.plan,
        s.monthly_fee,
        latest.order_item_quantity
    FROM attributed s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Grouped metrics
SELECT
    plan,
    store_id,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT(DISTINCT plan) AS distinct_plans,
    RANK() OVER (PARTITION BY plan ORDER BY COUNT(DISTINCT plan) DESC) AS distinct_plans_rank
FROM with_latest
GROUP BY plan, store_id
ORDER BY plan DESC NULLS FIRST
