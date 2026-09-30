WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        created_at,
        product_id,
        price,
        promo,
        element
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS element
),
base AS (
    -- Keep recent, valid rows
    SELECT
        created_at,
        product_id,
        price,
        promo,
        element,
        UPPER(promo) AS promo_upper
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-01 10:00:00'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share,
        ROUND(price / SUM(price) OVER (PARTITION BY element, promo), 4) AS price_share,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo, element), 4) AS product_id_share_2
    FROM base
    WHERE promo != 'VIP'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        product_id,
        promo_upper,
        product_id_share,
        AVG(price_share) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS price_share_avg_1h,
        PERCENT_RANK() OVER w_grp AS price_percent_rank,
        STDDEV_SAMP(product_id_share_2) OVER (PARTITION BY element, promo) AS element_stddev_samp_product_id_share_2
    FROM enriched
    WINDOW w_grp AS (PARTITION BY element ORDER BY price NULLS LAST)
)
SELECT
    product_id,
    promo_upper,
    product_id_share,
    price_share_avg_1h,
    price_percent_rank
FROM metrics
ORDER BY price_share_avg_1h NULLS FIRST;
