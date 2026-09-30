WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_grp AS tier_value_cume_dist,
        DENSE_RANK() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_dense_rank
    FROM flattened
    WINDOW w_grp AS (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        currency,
        tier_value_cume_dist,
        tier_value_dense_rank,
        CASE WHEN currency IN ('USD', 'EUR') THEN 'Core' ELSE 'Other' END AS currency_group,
        CAST(tier_value_dense_rank AS DOUBLE) AS tier_value_dense_rank_num
    FROM metrics
    WHERE currency IN ('THB', 'EUR', 'USD')
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(tier_value_dense_rank_num, 0) AS tier_value_dense_rank_num_filled
    FROM filtered
    WHERE tier_value_dense_rank_num < 65
)
-- Window metrics over several different partitions
SELECT
    *,
    RANK() OVER (ORDER BY tier_value_dense_rank_num DESC) AS tier_value_dense_rank_num_rank,
    CUME_DIST() OVER (ORDER BY tier_value_dense_rank_num_filled DESC NULLS LAST) AS tier_value_dense_rank_num_filled_cume_dist,
    MIN(tier_value_dense_rank_num) OVER (PARTITION BY currency) AS currency_min_tier_value_dense_rank_num,
    DENSE_RANK() OVER (ORDER BY tier_value_cume_dist DESC NULLS FIRST) AS tier_value_cume_dist_dense_rank
FROM cleaned
