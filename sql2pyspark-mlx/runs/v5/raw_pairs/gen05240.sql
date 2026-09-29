WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS element
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.city,
        c.age,
        c.country
    FROM lines a
    JOIN customers c
        ON a.customer_id <=> c.id
    WHERE c.name IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.element,
        src.city,
        src.country,
        p.credit_limit,
        p.segment
    FROM with_dims src
    LEFT JOIN customers p
        ON src.customer_id <=> p.id
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.city AS customer_city,
        c.age,
        c.credit_limit AS customer_credit_limit
    FROM enriched e
    JOIN customers c
        ON e.customer_id = c.id
)
SELECT
    customer_id,
    city,
    country,
    segment,
    customer_city,
    customer_credit_limit,
    CASE WHEN city RLIKE '^(Singapore|New York)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS city_zone
FROM with_dims_carts
ORDER BY customer_id NULLS FIRST, segment
