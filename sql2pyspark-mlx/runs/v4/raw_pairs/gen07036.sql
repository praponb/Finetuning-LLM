WITH latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.order_shipping_fee,
        top1.channel
    FROM customers s
    JOIN LATERAL (
            SELECT c.shipping_fee AS order_shipping_fee, c.channel
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.customer_id IS NULL
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.customer_id IN (5, 12, 15)
        )
),
kept_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
        )
)
-- Multi-level aggregation
SELECT
    channel,
    country,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT(*) FILTER (WHERE segment = 'Corporate') AS corporate_rows,
    ROUND(AVG(order_shipping_fee), 2) AS avg_order_shipping_fee
FROM kept_customers
GROUP BY 1, 2
HAVING ROUND(AVG(order_shipping_fee), 2) > 0
ORDER BY avg_order_shipping_fee DESC
