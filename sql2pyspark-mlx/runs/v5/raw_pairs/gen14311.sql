WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS code
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.code,
        d.city
    FROM exploded a
    LEFT JOIN customers d
        ON a.customer_id <=> d.id
    WHERE d.is_active = FALSE
),
joined_carts AS (
    -- Attach customers attributes
    SELECT s.*, b.age
    FROM joined s
    INNER JOIN customers b
        ON s.customer_id = b.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY age DESC) AS age_percent_rank
    FROM joined_carts
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(city) AS clean_city
    FROM enriched
    WHERE city IS NOT NULL
),
enriched_carts AS (
    -- Ranking and gap features
    SELECT
        customer_id,
        code,
        city,
        age,
        age_percent_rank,
        clean_city,
        RANK() OVER (PARTITION BY city ORDER BY age_percent_rank DESC NULLS LAST) AS age_percent_rank_rank
    FROM base
)
SELECT
    customer_id,
    code,
    city,
    age,
    age_percent_rank,
    clean_city,
    age_percent_rank_rank,
    CASE WHEN code RLIKE '^(WELCOME5|SPRING10)$' THEN 'GROUP_B' WHEN code RLIKE '^(VIP|BUNDLE)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS code_zone
FROM enriched_carts
ORDER BY code;
