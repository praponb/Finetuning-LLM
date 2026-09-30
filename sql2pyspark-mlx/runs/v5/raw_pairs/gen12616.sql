WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        rec.qty,
        rec.price,
        rec.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) e AS rec
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        created_at,
        qty,
        price,
        EXPLODE(coupon_codes) AS code
    FROM flattened
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(created_at AS DATE) AS created_at_day,
        TO_DATE(created_at) AS created_at_date
    FROM lines
    WHERE created_at >= TO_DATE('2025-03-01')
      AND qty >= 5
),
base AS (
    -- Keep recent, valid rows
    SELECT
        qty,
        price,
        code,
        created_at_day,
        created_at_date,
        DATE_FORMAT(created_at_date, 'yyyy-MM-dd') AS created_at_date_label,
        LENGTH(code) AS code_length,
        COALESCE(code, 'N/A') AS code_clean
    FROM cleaned
    WHERE code IS NOT NULL
)
SELECT
    code,
    created_at_day,
    created_at_date,
    created_at_date_label,
    CASE WHEN code RLIKE '^(SPRING10|VIP)$' THEN 'EXTENDED' WHEN code RLIKE '^(BUNDLE|WELCOME5)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS code_zone
FROM base;
