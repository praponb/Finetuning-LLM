WITH with_dims AS (
    -- Attach stores attributes
    SELECT s.*, p.sqft
    FROM orders s
    LEFT JOIN stores p
        ON s.store_id = p.id
    WHERE p.city = 'London'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        amount
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date, id) = 1
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.quantity
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM latest_match a
    JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.order_date >= sub.start_date
       AND a.order_date < sub.end_date + INTERVAL '7' DAY
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.customer_id,
        a.store_id,
        a.subscription_id,
        a.monthly_fee,
        b.store_name,
        b.region,
        b.city
    FROM with_window a
    LEFT JOIN stores b
        ON a.store_id = b.id
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.subscription_id,
    s.monthly_fee,
    s.store_name,
    s.region,
    s.city,
    CASE WHEN region RLIKE '^(North|Central)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS region_zone,
    (SELECT MIN(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS min_account_balance
FROM enriched s
ORDER BY subscription_id DESC NULLS FIRST, monthly_fee;
