WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TO_DATE(created_at) AS created_at_date,
        COALESCE(code, 'UNKNOWN') AS code_clean
    FROM lines
    WHERE code IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ADD_MONTHS(created_at_date, 6) AS created_at_date_plus_months,
        COALESCE(code, 'OTHER') AS code_filled
    FROM valid
    WHERE created_at > TIMESTAMP '2025-03-01 21:00:00'
      AND code IS NOT NULL
),
valid_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DAY(created_at) AS created_at_day
    FROM filtered
    WHERE created_at_date_plus_months BETWEEN '2024-04-18' AND '2024-11-12'
      AND code IS NOT NULL
)
SELECT
    id,
    created_at_date,
    code_clean,
    created_at_date_plus_months,
    code_filled,
    created_at_day
FROM valid_carts
ORDER BY created_at_date DESC NULLS FIRST, created_at_day;
