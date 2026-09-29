WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS val
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(effective_from, 'yyyy-MM') AS effective_from_label,
        CASE WHEN currency IN ('USD', 'THB') THEN 'Core' ELSE 'Other' END AS currency_group
    FROM flattened
    WHERE effective_from > DATE '2024-06-06'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY val NULLS LAST) AS val_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val NULLS FIRST) AS val_cume_dist,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val DESC) AS val_dense_rank,
        RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_rank_2
    FROM cleaned
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        effective_from,
        val,
        effective_from_label,
        val_rank,
        val_cume_dist,
        val_dense_rank,
        COALESCE(val_cume_dist, 0) AS val_cume_dist_filled,
        ROUND(val_rank * 1.15, 2) AS val_rank_adjusted
    FROM enriched
    WHERE effective_from > DATE '2024-07-28'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        effective_from,
        val,
        val_dense_rank,
        val_cume_dist_filled,
        val_rank_adjusted,
        ROUND(val_rank, 2) AS val_rank_rounded,
        val_cume_dist * val_cume_dist_filled AS val_cume_dist_times_val_cume_dist_filled
    FROM base
    WHERE effective_from >= TO_DATE('2024-08-29')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        val,
        val_dense_rank,
        val_cume_dist_filled,
        val_rank_adjusted,
        val_rank_rounded,
        val_cume_dist_times_val_cume_dist_filled,
        DENSE_RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_dense_rank_2,
        CUME_DIST() OVER (ORDER BY val DESC) AS val_cume_dist
    FROM filtered
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    APPROX_PERCENTILE(val_dense_rank, 0.9) AS val_dense_rank_p90,
    PERCENTILE_APPROX(val_rank_rounded, ARRAY(0.25, 0.5, 0.75)) AS val_rank_rounded_quartiles,
    APPROX_PERCENTILE(val_cume_dist_times_val_cume_dist_filled, 0.9) AS val_cume_dist_times_val_cume_dist_filled_p90,
    COUNT(*) AS row_count,
    ROUND(AVG(val_dense_rank_2), 2) AS avg_val_dense_rank_2
FROM metrics
GROUP BY DATE_TRUNC('QUARTER', effective_from)
ORDER BY val_dense_rank_p90
