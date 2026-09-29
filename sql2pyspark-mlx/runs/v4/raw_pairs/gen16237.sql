WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM carts
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(element) AS element_lower,
        INITCAP(TRIM(element)) AS element_title,
        COALESCE(element, 'N/A') AS element_filled
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-01 21:00:00'
      AND element IS NOT NULL
      AND element IN ('WELCOME5', 'SPRING10')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        element,
        element_lower,
        element_title,
        element_filled,
        COALESCE(element, 'unassigned') AS element_filled_2,
        TRIM(element) AS clean_element
    FROM cleaned
    WHERE created_at >= DATE '2025-03-01'
),
cleaned_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        MONTH(created_at) AS created_at_month,
        HOUR(created_at) AS created_at_hour,
        COALESCE(element, 'N/A') AS resolved_element
    FROM base
    WHERE element IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        element,
        element_title,
        element_filled,
        element_filled_2,
        created_at_month,
        created_at_hour,
        resolved_element,
        UPPER(element) AS element_upper
    FROM cleaned_carts
    WHERE created_at > TIMESTAMP '2025-03-02 10:00:00'
      AND element IS NOT NULL
),
base_carts AS (
    -- Keep recent, valid rows
    SELECT
        created_at,
        element,
        element_filled_2,
        created_at_hour,
        resolved_element,
        element_upper,
        DATE_TRUNC('month', created_at) AS created_at_period,
        DATE_TRUNC('year', created_at) AS created_at_period_2
    FROM valid
    WHERE created_at >= TIMESTAMP '2025-03-02 02:00:00'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', created_at_period) AS activity_month,
    ROUND(AVG(created_at_hour), 2) AS avg_created_at_hour,
    APPROX_PERCENTILE(created_at_hour, ARRAY(0.25, 0.5, 0.75)) AS created_at_hour_quartiles,
    MIN(created_at_period) AS first_seen
FROM base_carts
GROUP BY DATE_TRUNC('MONTH', created_at_period)
HAVING avg_created_at_hour >= 0
ORDER BY avg_created_at_hour DESC;
