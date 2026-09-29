WITH lines AS (
    SELECT
        id,
        line.product_id,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS line
),
enriched AS (
    SELECT
        *,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY product_id) AS product_id_dense_rank,
        PERCENT_RANK() OVER (ORDER BY product_id NULLS LAST) AS product_id_percent_rank,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share_2
    FROM lines
),
cleaned AS (
    SELECT
        id,
        product_id,
        price,
        promo,
        product_id_dense_rank,
        product_id_share_2,
        CASE WHEN product_id_dense_rank >= 8 THEN 'High' WHEN product_id_dense_rank >= 3 THEN 'Medium' ELSE 'Low' END AS product_id_dense_rank_level,
        COALESCE(promo, 'OTHER') AS resolved_promo
    FROM enriched
    WHERE product_id_share BETWEEN 0.43 AND 0.74
),
with_windows AS (
    SELECT
        id,
        product_id,
        price,
        promo,
        product_id_dense_rank,
        product_id_share_2,
        product_id_dense_rank_level,
        resolved_promo,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY product_id DESC NULLS LAST) AS product_id_percent_rank,
        COUNT(product_id_share_2) OVER (PARTITION BY promo) AS promo_count_product_id_share_2,
        MIN(product_id_dense_rank) OVER (PARTITION BY promo) AS promo_min_product_id_dense_rank
    FROM cleaned
),
enriched_carts AS (
    SELECT
        id,
        product_id,
        price,
        promo,
        product_id_dense_rank_level,
        promo_count_product_id_share_2,
        MIN(product_id_percent_rank) OVER (PARTITION BY promo) AS promo_min_product_id_percent_rank,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY product_id DESC NULLS FIRST) AS product_id_cume_dist,
        DENSE_RANK() OVER (ORDER BY promo_min_product_id_dense_rank NULLS FIRST) AS promo_min_product_id_dense_rank_dense_rank,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY promo_count_product_id_share_2) AS promo_count_product_id_share_2_cume_dist
    FROM with_windows
    WHERE promo_count_product_id_share_2 >= 9
)
SELECT
    id,
    product_id,
    price,
    promo,
    product_id_dense_rank_level,
    promo_count_product_id_share_2,
    promo_min_product_id_percent_rank,
    promo_count_product_id_share_2_cume_dist,
    CASE WHEN promo RLIKE '^(SPRING10|BUNDLE)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS promo_zone
FROM enriched_carts
ORDER BY promo_min_product_id_percent_rank DESC NULLS FIRST
