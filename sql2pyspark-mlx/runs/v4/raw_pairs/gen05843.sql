WITH flattened AS (
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
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        pos,
        promo,
        element
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) e AS element
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY element ORDER BY pos DESC) AS pos_percent_rank,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY pos DESC) AS pos_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY pos NULLS FIRST) AS pos_percent_rank_2,
        SUM(pos) OVER w_recent AS promo_sum_pos
    FROM lines
    WINDOW w_recent AS (PARTITION BY promo)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT DISTINCT
    SELECT id
    FROM carts
    WHERE customer_id = 2
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id
),
enriched_carts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY pos DESC NULLS LAST) AS pos_cume_dist,
        MAX(pos_percent_rank) OVER (PARTITION BY promo) AS promo_max_pos_percent_rank,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY promo_sum_pos) AS promo_sum_pos_dense_rank
    FROM kept_rows
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY pos_dense_rank NULLS FIRST) AS pos_dense_rank_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY promo_sum_pos_dense_rank DESC NULLS LAST) AS promo_sum_pos_dense_rank_percent_rank
    FROM enriched_carts
)
-- Multi-level aggregation
SELECT
    CASE WHEN promo_sum_pos_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS promo_sum_pos_dense_rank_tier,
    element,
    COUNT(*) FILTER (WHERE promo = 'VIP') AS vip_rows,
    MAX(promo_max_pos_percent_rank) FILTER (WHERE promo = 'BUNDLE') AS bundle_max_promo_max_pos_percent_rank,
    APPROX_PERCENTILE(pos_dense_rank_dense_rank, 0.95) AS pos_dense_rank_dense_rank_p95,
    GROUPING(CASE WHEN promo_sum_pos_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM with_windows
GROUP BY ROLLUP (CASE WHEN promo_sum_pos_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, element)
HAVING vip_rows >= 1
ORDER BY vip_rows DESC NULLS FIRST
