WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
),
enriched AS (
    -- Ranking and gap features
    SELECT *, PERCENT_RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_percent_rank
    FROM exploded
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, val_percent_rank + val AS val_percent_rank_plus_val
    FROM enriched
    WHERE val_percent_rank > 0.42
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY val DESC) AS val_percent_rank_2,
        PERCENT_RANK() OVER w_recent AS val_percent_rank_percent_rank
    FROM valid
    WINDOW w_recent AS (ORDER BY val_percent_rank DESC)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        val_percent_rank_2 * val_percent_rank_percent_rank AS val_percent_rank_2_times_val_percent_rank_percent_rank,
        val_percent_rank_plus_val - val_percent_rank_percent_rank AS val_percent_rank_plus_val_minus_val_percent_rank_percent_rank
    FROM metrics
    WHERE val_percent_rank >= 0.58
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, DENSE_RANK() OVER (ORDER BY val_percent_rank_2_times_val_percent_rank_percent_rank NULLS LAST) AS val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank
    FROM filtered
    WHERE val_percent_rank_2_times_val_percent_rank_percent_rank >= 25
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        val,
        val_percent_rank_plus_val,
        val_percent_rank_percent_rank,
        val_percent_rank_plus_val_minus_val_percent_rank_percent_rank,
        val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY val_percent_rank_plus_val DESC NULLS LAST) AS val_percent_rank_plus_val_percent_rank,
        CUME_DIST() OVER (ORDER BY val_percent_rank DESC) AS val_percent_rank_cume_dist,
        CUME_DIST() OVER (ORDER BY val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank DESC) AS val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank_cume_dist
    FROM scored
)
SELECT
    val,
    val_percent_rank_plus_val,
    val_percent_rank_plus_val_minus_val_percent_rank_percent_rank,
    val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank,
    val_percent_rank_plus_val_percent_rank,
    val_percent_rank_cume_dist,
    val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank_cume_dist
FROM metrics_price_tiers
ORDER BY val_percent_rank_2_times_val_percent_rank_percent_rank_dense_rank_cume_dist NULLS LAST;
