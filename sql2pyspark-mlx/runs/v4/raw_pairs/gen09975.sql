WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(coupon_codes) AS value
    FROM carts
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN value IN ('SPRING10', 'VIP') THEN 'Group A' ELSE 'Other' END AS value_group,
        COALESCE(value, 'UNKNOWN') AS value_filled
    FROM lines
    WHERE value IN ('VIP', 'SPRING10')
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(value)) AS value_title
    FROM cleaned
    WHERE value = 'SPRING10'
),
cleaned_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        value,
        value_filled,
        UPPER(value) AS value_upper
    FROM valid
    WHERE value IS NOT NULL
      AND value IS NULL
),
valid_carts AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(value, 'Unknown') AS value_filled_2,
        COALESCE(value, 'NONE') AS resolved_value
    FROM cleaned_carts
    WHERE value IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN value IN ('SPRING10', 'WELCOME5') THEN 'Group A' ELSE 'Other' END AS value_group
    FROM valid_carts
    WHERE value IS NOT NULL
),
valid_carts_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        value,
        value_filled,
        value_filled_2,
        resolved_value,
        value_group,
        LENGTH(value) AS value_length
    FROM base
    WHERE value = 'WELCOME5'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN value_length >= 87 THEN 'LARGE' ELSE 'SMALL' END AS value_length_tier,
    MAX(value_length) FILTER (WHERE value = 'VIP') AS vip_max_value_length,
    COUNT(*) AS cnt,
    ROUND(AVG(value_length), 2) AS avg_value_length
FROM valid_carts_2
GROUP BY CASE WHEN value_length >= 87 THEN 'LARGE' ELSE 'SMALL' END;
