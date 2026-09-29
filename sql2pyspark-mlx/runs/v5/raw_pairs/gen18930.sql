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
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, v
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        value
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS value
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.city,
        b.country,
        b.age
    FROM flattened s
    JOIN customers b
        ON s.customer_id <=> b.id
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.country AS customer_country,
        b.signup_date,
        b.credit_limit
    FROM joined a
    LEFT JOIN customers b
        ON a.customer_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    CASE WHEN age >= 57 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    MAX(age) FILTER (WHERE value = 'VIP') AS vip_max_age,
    APPROX_PERCENTILE(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    COUNT(*) FILTER (WHERE value = 'SPRING10') AS spring10_rows,
    ROUND(AVG(age), 2) AS avg_age,
    SIZE(COLLECT_SET(city)) AS num_cities,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('DAY', created_at) ORDER BY SIZE(COLLECT_SET(city)) DESC) AS num_cities_percent_rank
FROM joined_carts
GROUP BY DATE_TRUNC('DAY', created_at), CASE WHEN age >= 57 THEN 'HIGH' ELSE 'LOW' END;
