WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM (
        SELECT
            id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS line
        FROM carts
    ) x
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN price >= 249 THEN 'High' WHEN price >= 39 THEN 'Medium' ELSE 'Low' END AS price_band,
        product_id * qty AS product_id_times_qty
    FROM exploded
    WHERE created_at > DATE '2025-03-01'
      AND promo IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        created_at,
        product_id,
        qty,
        price,
        promo,
        price_band,
        product_id_times_qty,
        price + product_id AS price_plus_product_id,
        COALESCE(promo, 'N/A') AS promo_filled
    FROM base
    WHERE promo IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(qty) OVER (PARTITION BY promo) AS promo_avg_qty,
        ROUND(price / SUM(price) OVER (PARTITION BY promo), 4) AS price_share
    FROM valid
)
SELECT
    qty,
    promo,
    price_plus_product_id,
    promo_avg_qty,
    CASE WHEN promo RLIKE '^(BUNDLE|WELCOME5)$' THEN 'CORE' WHEN promo RLIKE '^(SPRING10|VIP)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS promo_zone
FROM metrics;
