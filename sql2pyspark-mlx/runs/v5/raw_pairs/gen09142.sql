WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) li AS element
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, TO_DATE(created_at) AS created_at_date
    FROM lines
    WHERE element IS NOT NULL
      AND element IS DISTINCT FROM 'WELCOME5'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.signup_date,
        p.name,
        p.segment
    FROM filtered e
    LEFT JOIN customers p
        ON e.customer_id = p.id
    WHERE p.city != 'Tokyo'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.created_at,
        e.element,
        e.signup_date,
        p.age,
        p.name AS customer_name,
        p.signup_date AS customer_signup_date
    FROM with_dims e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, p.city
    FROM enriched a
    LEFT JOIN customers p
        ON a.customer_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN age >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    PERCENTILE_APPROX(age, 0.9) AS age_p90,
    APPROX_PERCENTILE(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    SUM(CASE WHEN element = 'VIP' THEN age ELSE 0 END) AS vip_age,
    PERCENTILE_APPROX(age, 0.95) AS age_p95
FROM joined
GROUP BY age_tier, hour_window
ORDER BY hour_window
