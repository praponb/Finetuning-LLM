WITH base AS (
    SELECT *, LEAST(discount, shipping_fee) AS least_discount_shipping_fee
    FROM orders
    WHERE customer_id IS NOT NULL
      AND order_date BETWEEN '2023-04-01' AND '2024-05-01'
),
scored AS (
    SELECT
        *,
        MAX(discount) OVER (PARTITION BY store_id) AS store_id_max_discount,
        NTILE(3) OVER (PARTITION BY channel, store_id ORDER BY amount DESC, id) AS amount_bucket,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY discount) AS discount_dense_rank
    FROM base
),
not_flagged AS (
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.order_id IN (32, 35, 37, 38)
        )
),
eligible AS (
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.delivered_date < s.order_date
        )
),
with_latest AS (
    SELECT s.*, x.order_item_quantity
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_latest src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.order_date >= sub.start_date
       AND src.order_date < sub.end_date + INTERVAL 14 DAYS
)
SELECT *
FROM (
    SELECT
        plan,
        store_id,
        channel,
        discount
    FROM matched
) src
PIVOT (
    SUM(discount) AS value_sum,
    COUNT(1) AS cnt
    FOR channel IN ('mobile', 'web')
);
