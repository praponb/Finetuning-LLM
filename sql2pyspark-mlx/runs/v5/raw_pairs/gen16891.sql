WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        line.product_id,
        line.qty,
        line.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) e AS line
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        product_id,
        qty,
        promo,
        tag
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS tag
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(qty / SUM(qty) OVER (PARTITION BY tag), 4) AS qty_share,
        PERCENT_RANK() OVER (ORDER BY qty DESC NULLS LAST) AS qty_percent_rank,
        RANK() OVER (PARTITION BY tag ORDER BY product_id DESC NULLS FIRST) AS product_id_rank
    FROM exploded_carts
),
metrics AS (
    -- Ranking and gap features
    SELECT
        product_id,
        qty,
        promo,
        qty_share,
        qty_percent_rank,
        product_id_rank,
        PERCENT_RANK() OVER w_running AS qty_percent_rank_percent_rank
    FROM with_windows
    WINDOW w_running AS (PARTITION BY tag, promo ORDER BY qty_percent_rank DESC NULLS LAST)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN product_id_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS product_id_rank_tier,
    product_id,
    COUNT(*) FILTER (WHERE promo = 'VIP') AS vip_rows,
    COUNT(*) FILTER (WHERE promo = 'BUNDLE') AS bundle_rows
FROM metrics
GROUP BY CASE WHEN product_id_rank >= 7 THEN 'HIGH' ELSE 'LOW' END, product_id
ORDER BY product_id NULLS LAST
