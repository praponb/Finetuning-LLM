WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, attr_value
),
valid AS (
    SELECT
        customer_id,
        created_at,
        key,
        ARRAY_CONTAINS(coupon_codes, 'BUNDLE') AS is_bundle
    FROM exploded
    WHERE created_at > DATE '2025-03-01'
      AND customer_id IS NOT NULL
),
matched_keys AS (
    SELECT customer_id
    FROM valid
    INTERSECT
    SELECT customer_id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
),
kept_keys AS (
    SELECT customer_id
    FROM kept_rows
    EXCEPT
    SELECT customer_id
    FROM carts
    WHERE TO_DATE(created_at) >= '2023-07-01'
),
verified AS (
    SELECT s.*
    FROM kept_rows s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
joined AS (
    SELECT
        e.*,
        b.email,
        b.country,
        b.name
    FROM verified e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
base AS (
    SELECT
        *,
        INITCAP(TRIM(name)) AS name_title,
        COALESCE(country, 'OTHER') AS resolved_country
    FROM joined
    WHERE created_at > TIMESTAMP '2025-03-01 17:00:00'
      AND country IS NOT NULL
),
valid_carts AS (
    SELECT
        created_at,
        key,
        email,
        country,
        name,
        name_title,
        resolved_country,
        CONCAT_WS(',', email, key) AS email_key_key,
        COALESCE(key, 'UNKNOWN') AS resolved_key
    FROM base
    WHERE customer_id IS NOT NULL
      AND created_at > DATE '2025-03-01'
)
SELECT
    key,
    country,
    COUNT(*) AS num_rows,
    MAX(created_at) AS ended_at,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    MIN(created_at) AS started_at
FROM valid_carts
GROUP BY key, country
ORDER BY country DESC NULLS FIRST
