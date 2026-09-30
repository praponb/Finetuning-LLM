WITH filtered AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        discount,
        shipping_fee,
        discount * amount AS discount_times_amount
    FROM orders
    WHERE order_date > DATE '2023-06-14'
      AND channel IS NOT NULL
),
with_dims AS (
    SELECT
        e.*,
        b.opened_date,
        b.city,
        b.region
    FROM filtered e
    LEFT JOIN stores b
        ON e.store_id <=> b.id
),
matched AS (
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_dims a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.order_date >= sub.start_date
       AND a.order_date < sub.end_date + INTERVAL 14 DAYS
),
with_last AS (
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.order_date,
        s.shipping_fee,
        s.discount_times_amount,
        s.city,
        s.region,
        s.subscription_id,
        s.plan,
        s.monthly_fee,
        top1.shipment_delivered_date
    FROM matched s
    LEFT JOIN LATERAL (
            SELECT c.delivered_date AS shipment_delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_window AS (
    SELECT
        a.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_last a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.order_date >= sub.start_date
       AND a.order_date < sub.end_date + INTERVAL 7 DAYS
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.order_date,
    s.shipping_fee,
    s.discount_times_amount,
    s.subscription_id,
    s.monthly_fee,
    s.shipment_delivered_date,
    (SELECT MAX(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS max_order_amount
FROM with_window s
ORDER BY customer_id DESC;
