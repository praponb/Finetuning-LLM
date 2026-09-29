WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM carts
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(created_at AS DATE) AS created_at_day
    FROM lines
    WHERE created_at > TIMESTAMP '2025-03-02 00:00:00'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        created_at,
        value,
        CASE WHEN value IN ('VIP', 'BUNDLE') THEN 'Group A' ELSE 'Other' END AS value_group,
        ADD_MONTHS(created_at_day, 12) AS created_at_day_plus_months
    FROM valid
    WHERE HOUR(created_at) BETWEEN 8 AND 16
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.created_at,
        a.created_at_day_plus_months,
        b.city,
        b.name,
        b.country
    FROM base a
    LEFT JOIN customers b
        ON a.customer_id <=> b.id
)
SELECT
    id,
    customer_id,
    created_at_day_plus_months,
    city,
    name,
    country,
    CASE WHEN name RLIKE '^(Jack Taylor|Pim Chaiyaporn)$' THEN 'EXTENDED' WHEN name RLIKE '^(Alice Wong|bob smith)$' THEN 'PRIORITY' ELSE 'CORE' END AS name_zone
FROM enriched
