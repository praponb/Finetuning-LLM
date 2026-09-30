WITH exploded AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM (
        SELECT
            id,
            created_at,
            coupon_codes,
            EXPLODE_OUTER(items) AS elem
        FROM carts
    ) x
),
flattened AS (
    SELECT product_id, tag
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) t AS tag
),
enriched AS (
    SELECT
        product_id,
        tag,
        PERCENT_RANK() OVER (ORDER BY product_id DESC NULLS LAST) AS product_id_percent_rank,
        CUME_DIST() OVER (ORDER BY product_id DESC) AS product_id_cume_dist
    FROM flattened
),
enriched_carts AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY tag ORDER BY product_id_percent_rank) AS product_id_percent_rank_rank,
        RANK() OVER (ORDER BY product_id_cume_dist DESC) AS product_id_cume_dist_rank
    FROM enriched
),
base AS (
    SELECT
        *,
        product_id_percent_rank_rank * product_id AS product_id_percent_rank_rank_times_product_id,
        COALESCE(tag, 'NONE') AS tag_clean
    FROM enriched_carts
    WHERE product_id_cume_dist <= 0.38
)
SELECT
    tag,
    CASE WHEN product_id_cume_dist >= 0.78 THEN 'LARGE' ELSE 'SMALL' END AS product_id_cume_dist_tier,
    SORT_ARRAY(COLLECT_SET(tag)) AS tag_list,
    SORT_ARRAY(COLLECT_SET(tag)) AS tag_list_2,
    COUNT(*) AS cnt,
    GROUPING_ID() AS grouping_id
FROM base
GROUP BY GROUPING SETS ((tag, CASE WHEN product_id_cume_dist >= 0.78 THEN 'LARGE' ELSE 'SMALL' END), (tag))
HAVING COUNT(*) > 1
