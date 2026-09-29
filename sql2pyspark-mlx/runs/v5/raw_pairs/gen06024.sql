WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        line.product_id
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) x AS line
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(product_id AS STRING) AS product_id_str
    FROM exploded
    WHERE created_at > TIMESTAMP '2025-03-01 13:00:00'
      AND created_at < '2023-11-01'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM filtered
    INTERSECT
    SELECT id
    FROM carts
    WHERE TO_DATE(created_at) >= '2023-11-06'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SIZE(coupon_codes) AS coupon_codes_count,
        DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label
    FROM verified
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 18:00:00')
),
filtered_carts AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id,
        product_id_str,
        coupon_codes_count,
        created_at_label,
        SORT_ARRAY(coupon_codes) AS sorted_coupon_codes,
        CEIL(coupon_codes_count) AS coupon_codes_count_whole
    FROM base
    WHERE coupon_codes_count < 27
)
SELECT
    id,
    created_at,
    coupon_codes,
    product_id_str,
    coupon_codes_count_whole
FROM filtered_carts
ORDER BY coupon_codes_count_whole DESC, id;
