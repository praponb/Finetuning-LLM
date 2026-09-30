WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS element
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.created_at,
        a.element,
        d.credit_limit
    FROM exploded a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.element,
        a.credit_limit,
        c.name
    FROM enriched a
    LEFT JOIN customers c
        ON a.customer_id = c.id
    WHERE c.name LIKE '%Chai%'
),
enriched_carts AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(credit_limit) OVER (PARTITION BY element) AS element_min_credit_limit,
        CUME_DIST() OVER (ORDER BY credit_limit) AS credit_limit_cume_dist,
        RANK() OVER (ORDER BY credit_limit NULLS LAST) AS credit_limit_rank,
        AVG(credit_limit) OVER (PARTITION BY customer_id) AS customer_id_avg_credit_limit,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY element, customer_id), 4) AS credit_limit_share
    FROM joined
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.credit_limit,
        s.element_min_credit_limit,
        b.segment
    FROM enriched_carts s
    LEFT JOIN customers b
        ON s.customer_id = b.id
)
-- Ranking and gap features
SELECT
    customer_id,
    credit_limit,
    segment,
    RANK() OVER (ORDER BY element_min_credit_limit NULLS LAST) AS element_min_credit_limit_rank
FROM with_dims;
