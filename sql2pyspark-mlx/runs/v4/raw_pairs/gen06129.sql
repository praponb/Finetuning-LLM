WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) x AS pos, li_item
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN promo = 'BUNDLE' THEN 1 ELSE 0 END is_bundle,
        COALESCE(promo, 'NONE') resolved_promo
    FROM lines
    WHERE TO_DATE(created_at) >= '2024-03-25'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        qty,
        price,
        promo,
        is_bundle,
        resolved_promo,
        CUME_DIST() OVER w_ordered is_bundle_cume_dist,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) qty_share,
        ROUND(qty / SUM(qty) OVER (PARTITION BY customer_id), 4) qty_share_2,
        MAX(price) OVER w_ordered_2 price_max_1h
    FROM base
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY is_bundle NULLS FIRST), w_ordered_2 AS (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(pos) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) pos_count_1h,
        MAX(qty_share) OVER w_ordered promo_max_qty_share,
        DENSE_RANK() OVER w_grp price_max_1h_dense_rank,
        COUNT(qty_share) OVER w_part qty_share_count_30m,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY price NULLS LAST) price_dense_rank
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY promo, customer_id), w_grp AS (ORDER BY price_max_1h DESC NULLS LAST), w_part AS (PARTITION BY promo, customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN is_bundle >= 84 THEN 'HIGH' ELSE 'LOW' END AS is_bundle_tier,
        DATE_TRUNC('HOUR', created_at) AS hour_window,
        ROUND(SUM(price_max_1h_dense_rank) / NULLIF(COUNT(CASE WHEN promo = 'SPRING10' THEN 1 END), 0), 2) AS price_max_1h_dense_rank_per_spring10,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows
    GROUP BY CASE WHEN is_bundle >= 84 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('HOUR', created_at)
    HAVING distinct_ids >= 1
)
SELECT
    is_bundle_tier,
    hour_window,
    price_max_1h_dense_rank_per_spring10,
    distinct_ids
FROM grouped
ORDER BY price_max_1h_dense_rank_per_spring10 NULLS LAST;
