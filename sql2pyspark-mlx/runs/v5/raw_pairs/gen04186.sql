WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) t AS li_item
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        coupon_codes,
        product_id,
        qty,
        promo,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY product_id NULLS LAST) AS product_id_cume_dist,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY qty) AS qty_cume_dist,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share_2,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share_3
    FROM exploded
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(qty) AS qty_whole,
        COALESCE(promo, 'Unknown') AS promo_filled
    FROM enriched
    WHERE promo IS NOT NULL
),
enriched_carts AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(product_id_share_3) OVER (PARTITION BY promo) AS promo_stddev_samp_product_id_share_3,
        PERCENT_RANK() OVER (ORDER BY product_id_cume_dist) AS product_id_cume_dist_percent_rank
    FROM cleaned
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        coupon_codes,
        product_id,
        promo,
        product_id_cume_dist,
        qty_cume_dist,
        product_id_share_2,
        product_id_share_3,
        promo_stddev_samp_product_id_share_3,
        product_id_cume_dist - qty AS product_id_cume_dist_minus_qty,
        COALESCE(promo, 'N/A') AS promo_clean
    FROM enriched_carts
    WHERE promo IS NOT DISTINCT FROM 'WELCOME5'
),
report AS (
    SELECT
        product_id,
        promo,
        product_id_share_2,
        product_id_share_3,
        promo_stddev_samp_product_id_share_3,
        product_id_cume_dist_minus_qty,
        promo_clean
    FROM filtered
    ORDER BY promo_stddev_samp_product_id_share_3 DESC NULLS FIRST
)
SELECT
    promo,
    product_id_share_2,
    product_id_share_3,
    promo_stddev_samp_product_id_share_3,
    CASE WHEN promo RLIKE '^(WELCOME5|SPRING10)$' THEN 'CORE' WHEN promo RLIKE '^(VIP|BUNDLE)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS promo_zone
FROM report
ORDER BY promo DESC, promo_stddev_samp_product_id_share_3 DESC;
