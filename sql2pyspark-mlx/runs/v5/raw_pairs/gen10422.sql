WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS element
    FROM carts
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(element) AS element_upper,
        LENGTH(element) AS element_length,
        COALESCE(element, 'NONE') AS element_filled
    FROM flattened
    WHERE created_at >= DATE '2025-03-01'
      AND element IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.created_at,
        e.element_upper,
        e.element_length,
        e.element_filled,
        b.email,
        b.credit_limit
    FROM cleaned e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.element_upper,
        e.element_filled,
        p.signup_date,
        p.country,
        p.email AS customer_email
    FROM enriched e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        element_upper,
        element_filled,
        signup_date,
        country,
        customer_email,
        UPPER(customer_email) AS customer_email_upper
    FROM enriched_carts
    WHERE customer_id IS NOT NULL
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    country,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    COUNT_IF(country = 'TH') AS th_events,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(*) FILTER (WHERE country = 'TH') AS th_rows,
    GROUPING_ID() AS gid
FROM valid
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', signup_date), country), (DATE_TRUNC('QUARTER', signup_date)));
