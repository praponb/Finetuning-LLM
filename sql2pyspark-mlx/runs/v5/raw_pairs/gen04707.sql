WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS k, attr_value
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.signup_date,
        p.segment,
        p.name
    FROM flattened e
    JOIN customers p
        ON e.customer_id = p.id
    WHERE p.country = 'SG'
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.country,
        b.email,
        b.signup_date AS customer_signup_date
    FROM with_dims s
    JOIN customers b
        ON s.customer_id <=> b.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        attr_value,
        signup_date,
        segment,
        name,
        country,
        email,
        customer_signup_date,
        CASE WHEN k IN ('weight_kg', 'device') THEN 'Tier 1' ELSE 'Other' END AS k_group
    FROM joined
    WHERE country IS NOT NULL
)
SELECT
    s.id,
    s.customer_id,
    s.attr_value,
    s.signup_date,
    s.segment,
    s.name,
    s.country,
    s.email,
    s.customer_signup_date,
    (SELECT MAX(x.amount) FROM orders x WHERE x.customer_id = s.customer_id) AS max_order_amount
FROM valid s
ORDER BY customer_id DESC, signup_date
