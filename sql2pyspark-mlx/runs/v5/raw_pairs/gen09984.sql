WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.created_at,
        src.coupon_codes,
        elem.product_id,
        elem.price
    FROM carts src
    LATERAL VIEW OUTER EXPLODE(src.items) li AS elem
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        product_id,
        code
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) e AS code
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY code), 4) AS product_id_share,
        MAX(product_id) OVER (PARTITION BY code ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS product_id_max_2h,
        PERCENT_RANK() OVER (PARTITION BY code ORDER BY product_id NULLS FIRST) AS product_id_percent_rank
    FROM exploded
    WHERE created_at >= '2025-08-01'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(code) AS code_length,
        COALESCE(code, 'N/A') AS code_filled
    FROM metrics
    WHERE created_at >= TIMESTAMP '2025-03-02 02:00:00'
      AND code IS NOT NULL
),
metrics_carts AS (
    -- Running and moving aggregates
    SELECT
        created_at,
        product_id,
        product_id_max_2h,
        RANK() OVER w_running AS product_id_rank,
        AVG(product_id_max_2h) OVER (PARTITION BY code) AS code_avg_product_id_max_2h,
        RANK() OVER w_part AS code_length_rank
    FROM valid
    WINDOW w_running AS (PARTITION BY code ORDER BY product_id DESC NULLS FIRST), w_part AS (PARTITION BY code ORDER BY code_length DESC NULLS FIRST)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at,
        product_id,
        product_id_max_2h,
        product_id_rank,
        code_avg_product_id_max_2h,
        code_length_rank,
        TO_DATE(created_at) AS created_at_date
    FROM metrics_carts
    WHERE created_at >= TO_DATE('2025-03-01')
      AND TO_DATE(created_at) >= '2025-03-22'
)
SELECT
    product_id,
    product_id_max_2h,
    product_id_rank,
    code_avg_product_id_max_2h,
    code_length_rank,
    created_at_date
FROM filtered;
