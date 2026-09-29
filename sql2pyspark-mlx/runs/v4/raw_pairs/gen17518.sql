WITH lines AS (
    SELECT
        id,
        created_at,
        INLINE(items)
    FROM carts
),
with_windows AS (
    SELECT
        id,
        created_at,
        product_id,
        qty,
        price,
        promo,
        RANK() OVER (PARTITION BY promo ORDER BY qty DESC NULLS FIRST) AS qty_rank,
        COUNT(price) OVER w_grp AS price_count_1h,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share
    FROM lines
    WINDOW w_grp AS (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    SELECT id
    FROM with_windows
    INTERSECT
    SELECT id
    FROM carts
    WHERE customer_id IS NOT NULL
),
kept_rows AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
),
valid AS (
    SELECT
        id,
        created_at,
        product_id,
        qty,
        price,
        qty_rank,
        product_id_share,
        qty_rank - product_id_share AS qty_rank_minus_product_id_share,
        COALESCE(promo, 'UNKNOWN') AS promo_filled
    FROM kept_rows
    WHERE created_at > DATE '2025-03-01'
)
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    CASE WHEN product_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS product_id_tier,
    COUNT(*) AS cnt,
    AVG(qty_rank_minus_product_id_share) AS avg_qty_rank_minus_product_id_share
FROM valid
GROUP BY DATE_TRUNC('HOUR', created_at), CASE WHEN product_id >= 8 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(*) >= 1
ORDER BY cnt NULLS LAST;
