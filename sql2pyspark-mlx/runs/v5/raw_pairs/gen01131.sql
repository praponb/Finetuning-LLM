WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS code
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.city,
        d.age
    FROM lines e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        created_at,
        code,
        city,
        age,
        CAST(created_at AS DATE) AS created_at_day,
        COALESCE(age, 0) AS age_filled
    FROM enriched
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        code,
        age,
        created_at_day,
        age_filled,
        SUM(age) OVER (PARTITION BY code) AS code_sum_age,
        RANK() OVER (PARTITION BY city ORDER BY age DESC NULLS LAST) AS age_rank
    FROM filtered
    WHERE age_filled BETWEEN 27 AND 46
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY code ORDER BY age_filled DESC NULLS FIRST) AS age_filled_dense_rank,
        CUME_DIST() OVER (PARTITION BY code ORDER BY code_sum_age DESC NULLS LAST) AS code_sum_age_cume_dist,
        SUM(code_sum_age) OVER (PARTITION BY customer_id) AS customer_id_sum_code_sum_age,
        MAX(age_rank) OVER (PARTITION BY code) AS code_max_age_rank
    FROM with_windows
)
SELECT
    id,
    customer_id,
    code,
    age,
    created_at_day,
    age_filled,
    age_rank,
    age_filled_dense_rank,
    customer_id_sum_code_sum_age,
    CASE WHEN code RLIKE '^(SPRING10|BUNDLE)$' THEN 'EXTENDED' WHEN code RLIKE '^(VIP|WELCOME5)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS code_zone
FROM scored
ORDER BY id DESC;
