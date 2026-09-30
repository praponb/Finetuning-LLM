WITH base AS (
    SELECT
        *,
        CONCAT_WS('-', status, channel) AS status_channel_key,
        ROUND(shipping_fee, 2) AS shipping_fee_rounded
    FROM orders
    WHERE order_date > DATE '2023-06-06'
      AND amount > 1520
),
joined AS (
    SELECT
        a.*,
        p.sqft,
        p.store_name
    FROM base a
    LEFT JOIN stores p
        ON a.store_id = p.id
),
with_latest AS (
    SELECT s.*, x.unit_price
    FROM joined s
    JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.order_date,
    s.status,
    s.channel,
    s.amount,
    s.discount,
    s.shipping_fee,
    (SELECT SUM(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM with_latest s
ORDER BY customer_id, sum_subscription_monthly_fee;
