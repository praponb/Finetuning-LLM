WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM carts
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.age,
        b.country,
        b.credit_limit
    FROM exploded s
    LEFT JOIN customers b
        ON s.customer_id = b.id
    WHERE b.country = 'DE'
),
enriched_carts AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        AVG(credit_limit) OVER (PARTITION BY code) AS code_avg_credit_limit,
        MAX(age) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS age_max_30m,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY code), 4) AS credit_limit_share
    FROM enriched
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.code_avg_credit_limit,
        s.credit_limit_share,
        b.name,
        b.city,
        b.country
    FROM enriched_carts s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM joined
    EXCEPT ALL
    SELECT customer_id
    FROM carts
    WHERE SIZE(coupon_codes) > 2
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN matched_keys k ON s.customer_id = k.customer_id
)
-- Window metrics over several different partitions
SELECT
    id,
    code_avg_credit_limit,
    credit_limit_share,
    ROUND(code_avg_credit_limit / SUM(code_avg_credit_limit) OVER (PARTITION BY city), 4) AS code_avg_credit_limit_share,
    PERCENT_RANK() OVER w_grp AS code_avg_credit_limit_percent_rank,
    MIN(credit_limit_share) OVER (PARTITION BY country) AS country_min_credit_limit_share
FROM verified
WINDOW w_grp AS (PARTITION BY city ORDER BY code_avg_credit_limit DESC NULLS LAST);
