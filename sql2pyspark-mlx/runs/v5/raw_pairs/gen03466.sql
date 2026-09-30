WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM carts
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN value IN ('WELCOME5', 'BUNDLE') THEN 'Priority' ELSE 'Other' END value_group,
        DATE_FORMAT(created_at, 'yyyy-MM-dd') created_at_label
    FROM exploded
    WHERE customer_id IS NOT NULL
      AND TO_DATE(created_at) < '2023-04-06'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        created_at,
        value_group,
        created_at_label,
        DATE_FORMAT(created_at, 'yyyy-MM') created_at_label_2,
        COALESCE(value, 'OTHER') value_filled
    FROM base
    WHERE created_at >= DATE '2025-03-01'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.created_at,
        s.created_at_label,
        s.created_at_label_2,
        b.credit_limit,
        b.city
    FROM filtered s
    INNER JOIN customers b
        ON s.customer_id = b.id
)
-- Join a dimension and aggregate
SELECT
    c.country,
    e.city AS cart_city,
    COUNT(*) AS cnt,
    MAX(e.credit_limit) AS max_credit_limit,
    GROUPING_ID() AS gid
FROM enriched e
INNER JOIN customers c ON e.customer_id = c.id
GROUP BY GROUPING SETS ((c.country, e.city), (c.country))
ORDER BY country NULLS LAST;
