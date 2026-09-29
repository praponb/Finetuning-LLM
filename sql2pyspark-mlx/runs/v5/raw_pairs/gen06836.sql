WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        code
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) x AS code
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(code) AS clean_code
    FROM flattened
    WHERE code IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(code, 'unassigned') AS code_filled,
        COALESCE(code, 'N/A') AS resolved_code
    FROM valid
    WHERE created_at >= TO_TIMESTAMP('2025-03-02 03:00:00')
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        code,
        clean_code,
        code_filled,
        resolved_code,
        DAY(created_at) AS created_at_day,
        COALESCE(code, 'N/A') AS resolved_code_2
    FROM filtered
    WHERE created_at < '2025-02-01'
      AND code IS DISTINCT FROM 'WELCOME5'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN created_at_day >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END AS created_at_day_tier,
    COUNT_IF(code = 'BUNDLE') AS bundle_events,
    COUNT(*) AS num_rows,
    ROUND(SUM(created_at_day) / NULLIF(COUNT(CASE WHEN code = 'WELCOME5' THEN 1 END), 0), 2) AS created_at_day_per_welcome5
FROM valid_carts
GROUP BY ROLLUP (CASE WHEN created_at_day >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END);
