WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(code) AS code_lower
    FROM exploded
    WHERE customer_id IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(code) AS code_length
    FROM valid
    WHERE customer_id IS NOT NULL
      AND code IS NOT DISTINCT FROM 'VIP'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        code,
        code_lower,
        MAX(code_length) OVER (PARTITION BY code) AS code_max_code_length
    FROM base
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        code,
        code_lower,
        AVG(code_max_code_length) OVER (PARTITION BY code) AS code_avg_code_max_code_length,
        COUNT(code_max_code_length) OVER (PARTITION BY code) AS code_count_code_max_code_length
    FROM scored
    WHERE code = 'VIP'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN code_count_code_max_code_length >= 18 THEN 'High' WHEN code_count_code_max_code_length >= 6 THEN 'Medium' ELSE 'Low' END AS code_count_code_max_code_length_band
    FROM metrics
    WHERE code IS NOT NULL
      AND code IS DISTINCT FROM 'VIP'
)
SELECT
    code,
    code_lower,
    code_avg_code_max_code_length,
    code_count_code_max_code_length_band
FROM cleaned
ORDER BY code NULLS FIRST, code_avg_code_max_code_length
