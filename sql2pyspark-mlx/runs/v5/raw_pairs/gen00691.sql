WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, v
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM exploded
    EXCEPT
    SELECT customer_id, id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.coupon_codes,
        e.v,
        c.credit_limit
    FROM kept_rows e
    LEFT JOIN customers c
        ON e.customer_id = c.id
    WHERE c.name LIKE 'Gra%'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SIZE(coupon_codes) AS coupon_codes_count
    FROM joined
    WHERE customer_id IS NOT NULL
      AND ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(coupon_codes_count * 1.07, 2) AS coupon_codes_count_adjusted,
        ROUND(coupon_codes_count / credit_limit, 2) AS coupon_codes_count_per_credit_limit
    FROM cleaned
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN credit_limit >= 13100 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    CASE WHEN coupon_codes_count >= 71 THEN 'HIGH' ELSE 'LOW' END AS coupon_codes_count_tier,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count_2,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit
FROM base
GROUP BY GROUPING SETS ((CASE WHEN credit_limit >= 13100 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN coupon_codes_count >= 71 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN credit_limit >= 13100 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY row_count_2
