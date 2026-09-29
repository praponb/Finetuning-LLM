WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS code
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.age,
        c.email,
        c.credit_limit
    FROM lines e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
enriched AS (
    -- Attach customers attributes
    SELECT s.*, c.city
    FROM joined s
    JOIN customers c
        ON s.customer_id = c.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM enriched
    EXCEPT DISTINCT
    SELECT customer_id
    FROM carts
    WHERE TO_DATE(created_at) >= '2024-04-14'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.customer_id = k.customer_id
)
SELECT
    id,
    customer_id,
    created_at,
    code,
    age,
    email,
    credit_limit,
    city
FROM reconciled
ORDER BY code NULLS LAST;
