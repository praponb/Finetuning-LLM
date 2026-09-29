WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END AS is_shipped
    FROM orders
    WHERE channel IS NOT NULL
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM cleaned e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.order_date BETWEEN sub.start_date AND sub.end_date
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
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
        s.is_shipped,
        s.subscription_id,
        s.monthly_fee,
        top1.payment_status,
        top1.payment_amount
    FROM matched s
    LEFT JOIN LATERAL (
            SELECT c.status AS payment_status, c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        order_date,
        amount,
        discount,
        is_shipped,
        subscription_id,
        MIN(discount) OVER (PARTITION BY channel ORDER BY shipping_fee, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_discount,
        AVG(discount) OVER (PARTITION BY channel ORDER BY shipping_fee, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_discount,
        COUNT(discount) OVER (PARTITION BY channel ORDER BY shipping_fee, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_discount,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY payment_amount DESC, id, subscription_id) AS payment_amount_bucket
    FROM with_last
    WHERE customer_id IN (29, 32)
)
SELECT
    id,
    order_date,
    amount,
    is_shipped,
    subscription_id,
    last7_min_discount,
    last7_avg_discount
FROM scored
ORDER BY order_date DESC NULLS FIRST, is_shipped DESC
