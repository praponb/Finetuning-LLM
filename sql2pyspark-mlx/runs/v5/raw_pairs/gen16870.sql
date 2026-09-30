WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date < s.order_date
        )
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM kept src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.order_date >= sub.start_date
       AND src.order_date < sub.end_date + INTERVAL 7 DAYS
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM matched s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date BETWEEN pr.starts_on AND pr.ends_on
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.channel,
        s.shipping_fee,
        s.subscription_id,
        s.plan,
        s.monthly_fee,
        s.promo_id,
        x.payment_amount,
        x.payment_method
    FROM attributed s
    LEFT JOIN LATERAL (
            SELECT c.amount AS payment_amount, c.method AS payment_method
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Join a dimension and aggregate
SELECT
    p.country,
    s.channel,
    COUNT(*) AS num_rows,
    ROUND(SUM(s.payment_amount), 2) AS total_payment_amount,
    MAX(s.subscription_id) AS max_subscription_id,
    GROUPING_ID() AS level_id
FROM with_latest s
INNER JOIN customers p ON s.customer_id = p.id
GROUP BY GROUPING SETS ((p.country, s.channel), (p.country), ())
