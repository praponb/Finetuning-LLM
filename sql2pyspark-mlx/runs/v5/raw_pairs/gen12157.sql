WITH enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.plan,
        e.start_date,
        e.monthly_fee,
        d.email,
        d.country
    FROM subscriptions e
    LEFT JOIN customers d
        ON e.customer_id = d.id
    WHERE d.age <= 67
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.plan,
        a.start_date,
        a.country,
        p.name,
        p.country AS customer_country,
        p.signup_date
    FROM enriched a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.plan,
        src.start_date,
        src.country,
        src.customer_country,
        c.country AS customer_country_2
    FROM with_dims src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        country,
        customer_country,
        customer_country_2,
        LAST_VALUE(customer_country, TRUE) OVER (PARTITION BY country, customer_country_2 ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_customer_country,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM enriched_subscriptions
    WINDOW w_ordered AS (PARTITION BY country ORDER BY start_date, id)
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.start_date,
    s.customer_country,
    s.last_known_customer_country,
    s.event_seq,
    CASE WHEN plan RLIKE '^(premium|standard)$' THEN 'CORE' WHEN plan RLIKE '^(basic|enterprise)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS plan_zone,
    (SELECT MAX(o2.discount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS max_order_discount
FROM metrics s
ORDER BY max_order_discount NULLS FIRST, event_seq DESC
