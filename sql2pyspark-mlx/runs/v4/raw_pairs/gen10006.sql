WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS line
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        created_at,
        qty,
        EXPLODE(coupon_codes) AS element
    FROM flattened
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(qty) OVER w_hist AS qty_sum_1h,
        ROUND(qty / SUM(qty) OVER (PARTITION BY element), 4) AS qty_share,
        COUNT(qty) OVER (PARTITION BY element) AS element_count_qty,
        RANK() OVER (ORDER BY qty ASC) AS qty_rank
    FROM lines
    WINDOW w_hist AS (PARTITION BY element ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        created_at,
        qty,
        element,
        qty_sum_1h,
        qty_share,
        element_count_qty,
        qty_rank,
        ROUND(element_count_qty / SUM(element_count_qty) OVER (PARTITION BY element), 4) AS element_count_qty_share,
        PERCENT_RANK() OVER (PARTITION BY element ORDER BY element_count_qty DESC NULLS FIRST) AS element_count_qty_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY element ORDER BY qty_rank ASC NULLS FIRST) AS qty_rank_percent_rank,
        DENSE_RANK() OVER (PARTITION BY element ORDER BY qty_rank ASC NULLS LAST) AS qty_rank_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY element ORDER BY qty_share DESC NULLS LAST) AS qty_share_percent_rank
    FROM scored
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY element_count_qty_percent_rank ASC NULLS LAST) AS element_count_qty_percent_rank_rank,
        PERCENT_RANK() OVER (PARTITION BY element ORDER BY qty_rank_percent_rank DESC NULLS FIRST) AS qty_rank_percent_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY qty DESC NULLS FIRST) AS qty_cume_dist
    FROM with_windows
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        created_at,
        qty,
        qty_sum_1h,
        qty_share,
        qty_rank,
        element_count_qty_share,
        element_count_qty_percent_rank,
        qty_rank_percent_rank,
        qty_rank_dense_rank,
        qty_share_percent_rank,
        qty_rank_percent_rank_percent_rank,
        qty_cume_dist,
        qty_share + qty_rank AS qty_share_plus_qty_rank,
        LEAST(qty_cume_dist, qty_share_percent_rank) AS least_qty_cume_dist_qty_share_percent_rank
    FROM metrics
    WHERE qty_rank_dense_rank < 7
),
with_windows_carts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(qty_cume_dist) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS qty_cume_dist_max_30m,
        AVG(qty_rank_dense_rank) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS qty_rank_dense_rank_avg_15m,
        PERCENT_RANK() OVER w_ordered AS qty_share_plus_qty_rank_percent_rank
    FROM filtered
    WINDOW w_ordered AS (ORDER BY qty_share_plus_qty_rank DESC NULLS FIRST)
)
SELECT
    created_at,
    qty,
    qty_sum_1h,
    qty_share,
    element_count_qty_share,
    element_count_qty_percent_rank,
    qty_rank_dense_rank,
    qty_share_percent_rank,
    qty_rank_percent_rank_percent_rank
FROM with_windows_carts
ORDER BY element_count_qty_percent_rank DESC, qty_sum_1h ASC;
