WITH joined AS (
    SELECT
        src.*,
        d.region,
        d.store_name,
        d.sqft
    FROM orders src
    INNER JOIN stores d
        ON src.store_id = d.id
    WHERE d.city = 'Bangkok'
),
filtered AS (
    SELECT
        *,
        CASE WHEN shipping_fee >= 31 THEN 'High' WHEN shipping_fee >= 8 THEN 'Medium' ELSE 'Low' END AS shipping_fee_band,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM joined
    WHERE order_date > DATE '2023-07-15'
),
matched AS (
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM filtered a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.order_date BETWEEN sub.start_date AND sub.end_date
),
with_latest AS (
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.channel,
        s.shipping_fee,
        s.store_name,
        s.sqft,
        s.shipping_fee_band,
        s.subscription_id,
        s.plan,
        s.monthly_fee,
        x.order_item_quantity,
        x.order_item_unit_price
    FROM matched s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    SELECT *, LENGTH(plan) AS plan_length
    FROM with_latest
    WHERE order_item_unit_price BETWEEN 180 AND 270
),
with_last AS (
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.shipping_fee,
        s.store_name,
        s.sqft,
        s.subscription_id,
        s.plan,
        s.order_item_unit_price,
        s.plan_length,
        x.order_item_quantity_2,
        x.unit_price
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity_2, c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        id,
        shipping_fee,
        store_name,
        sqft,
        subscription_id,
        order_item_quantity_2,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY order_item_unit_price, id, subscription_id) AS order_item_unit_price_bucket,
        LEAD(plan_length, 1) OVER w_grp AS next_plan_length,
        NTILE(4) OVER w_grp AS unit_price_bucket
    FROM with_last
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY unit_price, id, subscription_id)
)
SELECT
    id,
    shipping_fee,
    store_name,
    sqft,
    subscription_id,
    order_item_quantity_2,
    order_item_unit_price_bucket,
    next_plan_length
FROM metrics
