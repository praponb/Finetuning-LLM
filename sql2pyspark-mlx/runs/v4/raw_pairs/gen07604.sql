WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.segment,
        d.city
    FROM flattened e
    JOIN customers d
        ON e.customer_id = d.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(city, ' - ', code) AS city_code,
        LOWER(code) AS code_lower,
        COALESCE(code, 'N/A') AS resolved_code
    FROM enriched
    WHERE code IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(segment) AS clean_segment
    FROM filtered
    WHERE city = 'New York'
)
-- Aggregate with conditional metrics
SELECT
    code,
    customer_id,
    COUNT_IF(code = 'SPRING10') AS spring10_events,
    COUNT(*) FILTER (WHERE segment = 'SMB') AS smb_rows,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    COUNT(*) FILTER (WHERE city = 'Singapore') AS singapore_rows
FROM valid
GROUP BY CUBE (code, customer_id)
ORDER BY spring10_events DESC
