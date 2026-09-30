WITH lines AS (
    SELECT
        id,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS val
),
enriched AS (
    SELECT
        id,
        effective_from,
        val,
        DENSE_RANK() OVER (ORDER BY val DESC) AS val_dense_rank,
        CUME_DIST() OVER (ORDER BY val) AS val_cume_dist,
        RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_rank
    FROM lines
),
base AS (
    SELECT
        id,
        effective_from,
        val_cume_dist,
        val_rank,
        val_rank + val AS val_rank_plus_val,
        val * val_rank AS val_times_val_rank
    FROM enriched
    WHERE effective_from > DATE '2024-03-10'
      AND val_rank > 6
),
filtered AS (
    SELECT
        id,
        val_cume_dist,
        val_rank,
        val_rank_plus_val,
        val_rank - val_cume_dist AS val_rank_minus_val_cume_dist,
        ROUND(val_times_val_rank * 1.1, 2) AS adjusted_val_times_val_rank
    FROM base
    WHERE val_rank < 6
),
with_windows AS (
    SELECT
        id,
        val_cume_dist,
        CUME_DIST() OVER (ORDER BY val_cume_dist DESC NULLS LAST) AS val_cume_dist_cume_dist,
        CUME_DIST() OVER (ORDER BY val_cume_dist DESC NULLS FIRST) AS val_cume_dist_cume_dist_2,
        PERCENT_RANK() OVER (ORDER BY val_rank NULLS FIRST) AS val_rank_percent_rank,
        PERCENT_RANK() OVER w_hist AS val_cume_dist_percent_rank,
        PERCENT_RANK() OVER w_hist_2 AS adjusted_val_times_val_rank_percent_rank
    FROM filtered
    WINDOW w_hist AS (ORDER BY val_cume_dist DESC), w_hist_2 AS (ORDER BY adjusted_val_times_val_rank DESC)
)
SELECT
    id,
    val_cume_dist_cume_dist_2,
    val_cume_dist_percent_rank,
    adjusted_val_times_val_rank_percent_rank
FROM with_windows
ORDER BY val_cume_dist_cume_dist_2 NULLS FIRST
