WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS k, v
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.signup_date,
        p.email,
        p.city
    FROM exploded a
    INNER JOIN customers p
        ON a.customer_id <=> p.id
    WHERE NOT p.is_active
),
metrics AS (
    -- Running and moving aggregates
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, customer_id ORDER BY signup_date DESC, id, k) AS event_seq
    FROM enriched
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(k) AS k_lower
    FROM metrics
    WHERE signup_date <= '2025-01-01'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.name,
        d.signup_date AS customer_signup_date
    FROM base e
    LEFT JOIN customers d
        ON e.customer_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', customer_signup_date) AS quarter_start,
    customer_id,
    COUNT(*) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_rows,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(DISTINCT k) AS distinct_ks,
    COUNT(CASE WHEN k = 'device' THEN 1 END) AS device_count,
    COUNT(*) FILTER (WHERE k = 'weight_kg') AS weight_kg_rows
FROM with_dims
GROUP BY 1, 2
ORDER BY weight_kg_rows DESC NULLS LAST
