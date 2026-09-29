WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM carts
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.signup_date,
        d.name,
        d.country
    FROM exploded src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.city,
        c.credit_limit,
        c.age
    FROM joined a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.credit_limit AS customer_credit_limit,
        b.age AS customer_age,
        b.country AS customer_country
    FROM joined_carts src
    LEFT JOIN customers b
        ON src.customer_id = b.id
)
SELECT
    code,
    signup_date,
    name,
    country,
    city,
    credit_limit,
    customer_credit_limit,
    CASE WHEN country RLIKE '^(DE|SG)$' THEN 'GROUP_B' WHEN country RLIKE '^(TH|US)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS country_zone
FROM with_dims
ORDER BY country_zone DESC NULLS LAST
