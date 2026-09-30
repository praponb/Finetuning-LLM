WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS elem
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        created_at,
        promo,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM lines
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(promo) AS promo_upper,
        COALESCE(code, 'OTHER') AS code_filled
    FROM exploded
    WHERE created_at >= TIMESTAMP '2025-03-01 21:00:00'
),
enriched AS (
    -- Attach customers attributes
    SELECT e.*, d.credit_limit
    FROM valid e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.country,
        c.signup_date
    FROM enriched a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
enriched_carts_2 AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.age,
        c.email,
        c.signup_date AS customer_signup_date
    FROM enriched_carts src
    LEFT JOIN customers c
        ON src.customer_id = c.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN age >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    CASE WHEN credit_limit >= 12700 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_tier,
    COUNT(*) FILTER (WHERE country = 'TH') AS th_rows,
    SUM(age) AS total_age
FROM enriched_carts_2
GROUP BY CASE WHEN age >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN credit_limit >= 12700 THEN 'HIGH' ELSE 'LOW' END;
