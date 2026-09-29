WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) e AS element
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        b.age,
        b.city
    FROM exploded a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        p.segment,
        p.signup_date,
        p.name
    FROM enriched s
    LEFT JOIN customers p
        ON s.customer_id <=> p.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.credit_limit,
        p.name AS customer_name
    FROM joined s
    JOIN customers p
        ON s.customer_id = p.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        segment,
        credit_limit,
        AVG(credit_limit) OVER (PARTITION BY customer_id) AS customer_id_avg_credit_limit
    FROM with_dims
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.segment,
        b.age
    FROM scored a
    JOIN customers b
        ON a.customer_id <=> b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        segment,
        age
    FROM joined_carts
) src
PIVOT (
    MAX(age) AS value_sum
    FOR segment IN ('Consumer', 'SMB')
);
