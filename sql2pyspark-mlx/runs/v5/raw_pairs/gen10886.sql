WITH joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.order_date,
        e.channel,
        e.amount,
        e.shipping_fee,
        p.segment,
        p.credit_limit
    FROM orders e
    LEFT JOIN customers p
        ON e.customer_id = p.id
    WHERE p.segment IN ('Consumer', 'Corporate')
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM joined
    EXCEPT ALL
    SELECT id, customer_id
    FROM orders
    WHERE shipping_fee < 22
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
joined_orders AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.order_date,
        e.amount,
        d.credit_limit AS customer_credit_limit,
        d.city
    FROM verified e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined_orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.order_id IN (4, 25, 26, 37)
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.email,
        p.signup_date,
        p.credit_limit
    FROM eligible src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.age,
        c.city AS customer_city
    FROM with_dims s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        amount,
        customer_credit_limit,
        city,
        signup_date,
        credit_limit,
        age,
        customer_city,
        SUBSTRING(customer_city, 1, 3) AS customer_city_prefix
    FROM enriched
    WHERE city IS NOT NULL
)
SELECT
    id,
    customer_id,
    customer_credit_limit,
    city,
    signup_date,
    credit_limit,
    age,
    customer_city,
    CASE WHEN city RLIKE '^(Tokyo|Chiang Mai)$' THEN 'CORE' ELSE 'GROUP_B' END AS city_zone
FROM filtered
ORDER BY customer_credit_limit ASC NULLS LAST
