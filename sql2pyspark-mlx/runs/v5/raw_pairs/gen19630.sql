WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN country = 'JP' THEN 1 ELSE 0 END AS is_jp
    FROM customers
    WHERE signup_date > TO_DATE('2023-09-23')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC, id) AS rank_in_group
        FROM filtered
    ) x
    WHERE rank_in_group = 1
),
latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.country,
        s.segment,
        s.signup_date,
        s.age,
        s.is_jp,
        s.rank_in_group,
        x.subscription_status
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.status AS subscription_status
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match_customers AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_shipping_fee
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.shipping_fee AS order_shipping_fee
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    name,
    country,
    signup_date,
    is_jp,
    rank_in_group,
    subscription_status,
    CASE WHEN subscription_status RLIKE '^(ACTIVE|EXPIRED)$' THEN 'CORE' WHEN subscription_status RLIKE '^(CANCELLED|PAUSED)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS subscription_status_zone
FROM latest_match_customers
