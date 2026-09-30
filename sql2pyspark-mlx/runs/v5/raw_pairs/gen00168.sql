WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS element
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(element)) AS element_title
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-01 21:00:00'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(created_at AS DATE) AS created_at_day,
        CAST(created_at AS DATE) AS created_at_day_2,
        COALESCE(element, 'N/A') AS element_filled
    FROM valid
    WHERE customer_id IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.element,
        src.created_at_day,
        src.element_filled,
        b.signup_date,
        b.city
    FROM cleaned src
    LEFT JOIN customers b
        ON src.customer_id = b.id
    WHERE b.is_active = FALSE
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id, id
    FROM joined
    INTERSECT ALL
    SELECT customer_id, id
    FROM carts
    WHERE SIZE(coupon_codes) >= 2
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN matched_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.segment,
        p.city AS customer_city
    FROM reconciled s
    JOIN customers p
        ON s.customer_id <=> p.id
)
SELECT
    customer_id,
    element,
    element_filled,
    city,
    segment,
    customer_city,
    CASE WHEN segment RLIKE '^(Consumer|SMB)$' THEN 'CORE' ELSE 'PRIORITY' END AS segment_zone
FROM enriched
ORDER BY customer_id DESC NULLS FIRST
