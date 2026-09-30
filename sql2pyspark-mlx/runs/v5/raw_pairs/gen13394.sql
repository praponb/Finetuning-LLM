WITH lines AS (
    SELECT
        id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
exploded AS (
    SELECT
        product_id,
        promo,
        EXPLODE(coupon_codes) AS tag
    FROM lines
),
scored AS (
    SELECT
        product_id,
        tag,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share,
        CUME_DIST() OVER (PARTITION BY tag ORDER BY product_id DESC NULLS FIRST) AS product_id_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY product_id DESC NULLS LAST) AS product_id_percent_rank
    FROM exploded
),
enriched AS (
    SELECT
        product_id,
        product_id_share,
        product_id_cume_dist,
        STDDEV_SAMP(product_id_percent_rank) OVER w_ordered AS tag_stddev_samp_product_id_percent_rank,
        MIN(product_id_percent_rank) OVER w_ordered AS tag_min_product_id_percent_rank,
        AVG(product_id_percent_rank) OVER w_ordered AS tag_avg_product_id_percent_rank
    FROM scored
    WHERE tag <> 'SPRING10'
    WINDOW w_ordered AS (PARTITION BY tag)
),
valid AS (
    SELECT *, CASE WHEN tag_avg_product_id_percent_rank >= 1 THEN 'High' WHEN tag_avg_product_id_percent_rank >= 0 THEN 'Medium' ELSE 'Low' END AS tag_avg_product_id_percent_rank_band
    FROM enriched
    WHERE tag_stddev_samp_product_id_percent_rank >= 0
),
scored_carts AS (
    SELECT *, PERCENT_RANK() OVER (ORDER BY product_id_share DESC) AS product_id_share_percent_rank
    FROM valid
)
SELECT
    product_id,
    CASE WHEN product_id_share_percent_rank >= 0.79 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_id_share_percent_rank_tier,
    PERCENTILE_APPROX(tag_stddev_samp_product_id_percent_rank, 0.9) AS tag_stddev_samp_product_id_percent_rank_p90,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(product_id_share_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS product_id_share_percent_rank_quartiles,
    COUNT(*) AS row_count,
    ROUND(AVG(product_id), 2) AS avg_product_id,
    ROUND(AVG(tag_stddev_samp_product_id_percent_rank), 2) AS avg_tag_stddev_samp_product_id_percent_rank
FROM scored_carts
GROUP BY GROUPING SETS ((product_id, CASE WHEN product_id_share_percent_rank >= 0.79 THEN 'PREMIUM' ELSE 'STANDARD' END), (product_id), ())
