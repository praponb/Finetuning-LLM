WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        li_item.product_id,
        li_item.qty,
        li_item.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) li AS li_item
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        created_at,
        product_id,
        qty,
        promo,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) AS qty_share,
        COUNT(qty) OVER (PARTITION BY promo) AS promo_count_qty
    FROM lines
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY qty DESC) AS qty_dense_rank,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY promo_count_qty NULLS FIRST) AS promo_count_qty_dense_rank,
        SUM(promo_count_qty) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS promo_count_qty_sum_1h,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) AS qty_share_2,
        MAX(qty) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS qty_max_2h
    FROM with_windows
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        product_id,
        qty,
        promo,
        qty_share,
        promo_count_qty,
        qty_dense_rank,
        promo_count_qty_dense_rank,
        qty_share_2,
        CEIL(qty) AS qty_whole,
        COALESCE(promo, 'DIRECT') AS promo_clean
    FROM metrics
    WHERE created_at >= DATE '2025-03-01'
      AND promo IS NOT NULL
      AND qty_dense_rank > 7
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(product_id / qty, 2) AS product_id_per_qty,
        COALESCE(promo, 'DIRECT') AS resolved_promo
    FROM base
    WHERE created_at >= TO_DATE('2025-03-02')
      AND promo IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY qty_share_2 DESC NULLS FIRST) AS qty_share_2_percent_rank,
        PERCENT_RANK() OVER w_grp AS qty_share_percent_rank,
        SUM(qty_share_2) OVER w_running AS promo_sum_qty_share_2,
        COUNT(promo_count_qty) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS promo_count_qty_count_2h,
        ROUND(promo_count_qty / SUM(promo_count_qty) OVER (PARTITION BY promo), 4) AS promo_count_qty_share
    FROM filtered
    WINDOW w_grp AS (PARTITION BY promo ORDER BY qty_share DESC NULLS LAST), w_running AS (PARTITION BY promo)
),
enriched_carts AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        product_id,
        qty,
        promo,
        qty_share,
        promo_count_qty,
        qty_dense_rank,
        promo_count_qty_dense_rank,
        qty_share_2,
        qty_whole,
        promo_clean,
        product_id_per_qty,
        resolved_promo,
        qty_share_2_percent_rank,
        qty_share_percent_rank,
        promo_sum_qty_share_2,
        promo_count_qty_count_2h,
        promo_count_qty_share,
        MAX(product_id_per_qty) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS product_id_per_qty_max_15m,
        MIN(promo_count_qty) OVER (PARTITION BY promo) AS promo_min_promo_count_qty,
        SUM(qty_share_percent_rank) OVER (PARTITION BY promo) AS promo_sum_qty_share_percent_rank,
        MIN(qty_share_2_percent_rank) OVER (PARTITION BY promo) AS promo_min_qty_share_2_percent_rank,
        ROUND(promo_count_qty / SUM(promo_count_qty) OVER (PARTITION BY promo), 4) AS promo_count_qty_share_2
    FROM enriched
)
-- Multi-level aggregation
SELECT
    CASE WHEN qty_share_2 >= 0.62 THEN 'LARGE' ELSE 'SMALL' END AS qty_share_2_tier,
    ROUND(SUM(product_id_per_qty) / NULLIF(COUNT(CASE WHEN promo = 'BUNDLE' THEN 1 END), 0), 2) AS product_id_per_qty_per_bundle,
    COUNT(*) FILTER (WHERE promo = 'BUNDLE') AS bundle_rows,
    SORT_ARRAY(COLLECT_SET(promo)) AS promo_list,
    COUNT_IF(promo = 'WELCOME5') AS welcome5_events,
    MIN(promo_count_qty) AS min_promo_count_qty
FROM enriched_carts
GROUP BY CASE WHEN qty_share_2 >= 0.62 THEN 'LARGE' ELSE 'SMALL' END
