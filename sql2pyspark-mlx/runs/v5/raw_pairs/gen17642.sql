WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, attr_value
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        attr_value,
        EXPLODE(coupon_codes) AS tag
    FROM flattened
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(tag)) AS tag_title
    FROM flattened_carts
    WHERE customer_id IS NOT NULL
      AND attr_key IS DISTINCT FROM 'utm_source'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.age,
        p.city
    FROM filtered s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT *, SUM(age) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS age_sum_1h
    FROM enriched
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(age_sum_1h, 2) AS age_sum_1h_rounded,
        COALESCE(city, 'OTHER') AS city_clean
    FROM scored
    WHERE created_at >= TIMESTAMP '2025-03-01 20:00:00'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM base
    INTERSECT
    SELECT customer_id
    FROM carts
    WHERE TO_DATE(created_at) < '2023-12-23'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
)
SELECT
    s.id,
    s.customer_id,
    s.created_at,
    s.attr_key,
    s.attr_value,
    s.tag,
    s.tag_title,
    s.age,
    s.city,
    (SELECT SUM(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM verified s
ORDER BY tag NULLS FIRST, city DESC
