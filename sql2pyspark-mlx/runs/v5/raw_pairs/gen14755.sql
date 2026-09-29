WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) e AS val
),
enriched AS (
    -- Running and moving aggregates
    SELECT *, CUME_DIST() OVER (ORDER BY val NULLS FIRST) AS val_cume_dist
    FROM lines
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        currency,
        val,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val_cume_dist DESC NULLS FIRST) AS val_cume_dist_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_cume_dist DESC) AS val_cume_dist_percent_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val NULLS LAST) AS val_cume_dist_2,
        MIN(val) OVER (PARTITION BY currency) AS currency_min_val,
        AVG(val_cume_dist) OVER (PARTITION BY currency) AS currency_avg_val_cume_dist
    FROM enriched
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(currency_avg_val_cume_dist) OVER (PARTITION BY currency) AS currency_sum_currency_avg_val_cume_dist,
        PERCENT_RANK() OVER w_running AS currency_min_val_percent_rank,
        RANK() OVER w_running AS currency_min_val_rank
    FROM metrics
    WHERE val_cume_dist_2 >= 0.58
    WINDOW w_running AS (PARTITION BY currency ORDER BY currency_min_val DESC NULLS LAST)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        val_cume_dist_dense_rank,
        val_cume_dist_percent_rank,
        val_cume_dist_2,
        currency_avg_val_cume_dist,
        currency_sum_currency_avg_val_cume_dist,
        currency_min_val_percent_rank,
        currency_min_val_rank,
        GREATEST(currency_min_val_rank, val_cume_dist_percent_rank) AS greatest_currency_min_val_rank_val_cume_dist_percent_rank,
        CAST(val_cume_dist_2 AS INT) AS val_cume_dist_2_int
    FROM with_windows
    WHERE val_cume_dist_percent_rank <= 0.3
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN currency_min_val_rank >= 6 THEN 'HIGH' ELSE 'LOW' END AS currency_min_val_rank_tier,
    MAX(val_cume_dist_2_int) AS max_val_cume_dist_2_int,
    MIN(val_cume_dist_percent_rank) AS min_val_cume_dist_percent_rank,
    APPROX_PERCENTILE(val_cume_dist_2_int, ARRAY(0.25, 0.5, 0.75)) AS val_cume_dist_2_int_quartiles
FROM valid
GROUP BY CASE WHEN currency_min_val_rank >= 6 THEN 'HIGH' ELSE 'LOW' END
HAVING MIN(val_cume_dist_percent_rank) > 0
ORDER BY max_val_cume_dist_2_int DESC
