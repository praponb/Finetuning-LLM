WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM carts
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(code) AS code_length,
        DATE_FORMAT(created_at, 'MMM yyyy') AS created_at_label,
        COALESCE(code, 'NONE') AS code_filled
    FROM flattened
    WHERE code IS NOT NULL
),
base_carts AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        code_length,
        created_at_label,
        code_filled,
        COALESCE(code_length, 0) AS code_length_filled,
        CEIL(code_length) AS code_length_whole,
        COALESCE(code, 'N/A') AS code_filled_2
    FROM base
    WHERE created_at >= TIMESTAMP '2025-03-01 19:00:00'
      AND code IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        created_at,
        code_length,
        created_at_label,
        code_filled,
        code_length_filled,
        code_length_whole,
        code_filled_2,
        MAX(code_length_filled) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS code_length_filled_max_15m,
        COUNT(code_length_filled) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS code_length_filled_count_1h
    FROM base_carts
)
SELECT
    id,
    created_at_label,
    code_filled,
    code_length_filled,
    code_length_whole,
    code_filled_2,
    code_length_filled_max_15m,
    code_length_filled_count_1h
FROM metrics
ORDER BY code_length_filled_count_1h NULLS FIRST;
