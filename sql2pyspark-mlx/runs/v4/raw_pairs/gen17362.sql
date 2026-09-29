WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        pos,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) e AS pos, elem
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        coupon_codes,
        pos,
        product_id,
        qty,
        price,
        promo,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share,
        NTILE(3) OVER w_hist AS price_bucket,
        COUNT(pos) OVER w_recent AS promo_count_pos,
        STDDEV_SAMP(price) OVER w_recent AS promo_stddev_samp_price,
        LAG(product_id, 1, 0) OVER (PARTITION BY promo ORDER BY pos, id, pos) AS prev_product_id
    FROM exploded
    WINDOW w_hist AS (PARTITION BY promo ORDER BY price DESC, id, pos), w_recent AS (PARTITION BY promo)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        coupon_codes,
        pos,
        product_id,
        promo,
        promo_count_pos,
        promo_stddev_samp_price,
        LEAST(prev_product_id, pos) AS least_prev_product_id_pos
    FROM enriched
    WHERE product_id IS NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        coupon_codes,
        pos,
        product_id,
        promo,
        least_prev_product_id_pos,
        ROUND(promo_count_pos / promo_stddev_samp_price, 2) AS promo_count_pos_per_promo_stddev_samp_price,
        ARRAY_JOIN(coupon_codes, ', ') AS coupon_codes_text,
        COALESCE(promo, 'N/A') AS promo_filled
    FROM cleaned
    WHERE promo IS NOT NULL
      AND promo_stddev_samp_price <= 106
)
-- Grouped metrics
SELECT
    promo,
    COUNT(*) FILTER (WHERE promo = 'SPRING10') AS spring10_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN promo = 'BUNDLE' THEN promo_count_pos_per_promo_stddev_samp_price ELSE 0 END) AS bundle_promo_count_pos_per_promo_stddev_samp_price,
    MAX(promo_count_pos_per_promo_stddev_samp_price) FILTER (WHERE promo = 'VIP') AS vip_max_promo_count_pos_per_promo_stddev_samp_price,
    ROUND(AVG(promo_count_pos_per_promo_stddev_samp_price), 2) AS avg_promo_count_pos_per_promo_stddev_samp_price,
    PERCENTILE_APPROX(pos, 0.95) AS pos_p95
FROM filtered
GROUP BY 1
HAVING COUNT(*) FILTER (WHERE promo = 'SPRING10') < 1;
