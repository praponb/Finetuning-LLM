WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) e AS val
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(val AS INT) AS val_int
    FROM exploded
    WHERE val > 66
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        val,
        val_int,
        PERCENT_RANK() OVER w_grp AS val_int_percent_rank,
        DENSE_RANK() OVER w_grp AS val_int_dense_rank,
        DENSE_RANK() OVER w_hist AS val_dense_rank
    FROM base
    WINDOW w_grp AS (ORDER BY val_int DESC NULLS LAST), w_hist AS (ORDER BY val DESC)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        val_int,
        val_int_percent_rank,
        val_int_dense_rank,
        val_dense_rank,
        PERCENT_RANK() OVER (ORDER BY val_int_percent_rank DESC) AS val_int_percent_rank_percent_rank
    FROM enriched
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        val_int,
        val_int_percent_rank,
        val_int_dense_rank,
        val_dense_rank,
        val_int_percent_rank_percent_rank,
        val_int_percent_rank_percent_rank / NULLIF(val_int, 0) AS val_int_percent_rank_percent_rank_per_val_int,
        val_int_percent_rank + val_dense_rank AS val_int_percent_rank_plus_val_dense_rank
    FROM with_windows
    WHERE val_int_percent_rank_percent_rank > 0.34
),
filtered_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(val_int_percent_rank, 0) AS val_int_percent_rank_filled,
        CASE WHEN val_int_dense_rank >= 8 THEN 'High' WHEN val_int_dense_rank >= 3 THEN 'Medium' ELSE 'Low' END AS val_int_dense_rank_level
    FROM filtered
    WHERE val_int_percent_rank_plus_val_dense_rank <= 67
),
with_windows_price_tiers AS (
    -- Ranking and gap features
    SELECT *, PERCENT_RANK() OVER (ORDER BY val_int_percent_rank) AS val_int_percent_rank_percent_rank_2
    FROM filtered_price_tiers
    WHERE val_int_percent_rank_plus_val_dense_rank < 35
)
-- Grouped metrics
SELECT
    CASE WHEN val_int_percent_rank >= 0.85 THEN 'HIGH' ELSE 'LOW' END AS val_int_percent_rank_tier,
    CASE WHEN val_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS val_dense_rank_tier,
    PERCENTILE_APPROX(val_int, 0.9) AS val_int_p90,
    PERCENTILE_APPROX(val_int_percent_rank_percent_rank, 0.9) AS val_int_percent_rank_percent_rank_p90,
    APPROX_PERCENTILE(val_int_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS val_int_dense_rank_quartiles
FROM with_windows_price_tiers
GROUP BY val_int_percent_rank_tier, val_dense_rank_tier
HAVING PERCENTILE_APPROX(val_int_percent_rank_percent_rank, 0.9) < 1000000
ORDER BY val_int_percent_rank_percent_rank_p90
