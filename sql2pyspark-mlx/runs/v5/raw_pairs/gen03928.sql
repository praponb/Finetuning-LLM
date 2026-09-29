WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM carts
),
with_dims AS (
    SELECT
        e.*,
        d.name,
        d.segment,
        d.signup_date
    FROM exploded e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
with_dims_carts AS (
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        src.segment,
        d.name AS customer_name,
        d.credit_limit
    FROM with_dims src
    INNER JOIN customers d
        ON src.customer_id = d.id
),
scored AS (
    SELECT
        id,
        customer_id,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 4) AS credit_limit_share,
        AVG(credit_limit) OVER (PARTITION BY segment) AS segment_avg_credit_limit
    FROM with_dims_carts
),
enriched AS (
    SELECT
        src.id,
        src.customer_id,
        src.credit_limit_share,
        src.segment_avg_credit_limit,
        c.credit_limit,
        c.segment
    FROM scored src
    LEFT JOIN customers c
        ON src.customer_id <=> c.id
    WHERE c.city IN ('Bangkok', 'Berlin')
),
joined AS (
    SELECT
        s.*,
        b.email,
        b.name
    FROM enriched s
    INNER JOIN customers b
        ON s.customer_id = b.id
),
with_dims_carts_2 AS (
    SELECT s.*, b.name AS customer_name
    FROM joined s
    INNER JOIN customers b
        ON s.customer_id = b.id
)
SELECT
    credit_limit_share,
    segment_avg_credit_limit,
    credit_limit,
    segment,
    email,
    name
FROM with_dims_carts_2
ORDER BY segment NULLS FIRST
