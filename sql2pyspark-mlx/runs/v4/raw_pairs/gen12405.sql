WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS tier_value
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        tier_value,
        CUME_DIST() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_cume_dist,
        RANK() OVER w_hist AS tier_value_rank
    FROM flattened
    WINDOW w_hist AS (ORDER BY tier_value DESC NULLS LAST)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        tier_value,
        tier_value_cume_dist,
        tier_value_rank,
        PERCENT_RANK() OVER w_running AS tier_value_percent_rank,
        CUME_DIST() OVER (ORDER BY tier_value_cume_dist DESC NULLS LAST) AS tier_value_cume_dist_cume_dist,
        PERCENT_RANK() OVER w_running AS tier_value_percent_rank_2,
        PERCENT_RANK() OVER w_part AS tier_value_percent_rank_3,
        RANK() OVER (ORDER BY tier_value_cume_dist NULLS FIRST) AS tier_value_cume_dist_rank
    FROM enriched
    WINDOW w_running AS (ORDER BY tier_value DESC), w_part AS (ORDER BY tier_value NULLS LAST)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER w_recent AS tier_value_percent_rank_3_dense_rank,
        PERCENT_RANK() OVER w_recent_2 AS tier_value_percent_rank_3_percent_rank
    FROM scored
    WINDOW w_recent AS (ORDER BY tier_value_percent_rank_3 NULLS LAST), w_recent_2 AS (ORDER BY tier_value_percent_rank_3 DESC NULLS LAST)
),
scored_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_grp AS tier_value_percent_rank_3_dense_rank_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_percent_rank_3_dense_rank DESC NULLS LAST) AS tier_value_percent_rank_3_dense_rank_percent_rank
    FROM metrics
    WINDOW w_grp AS (ORDER BY tier_value_percent_rank_3_dense_rank DESC)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tier_value_rank NULLS LAST) AS tier_value_rank_dense_rank,
        DENSE_RANK() OVER w_running AS tier_value_percent_rank_3_dense_rank_2,
        PERCENT_RANK() OVER (ORDER BY tier_value_cume_dist_rank NULLS LAST) AS tier_value_cume_dist_rank_percent_rank
    FROM scored_price_tiers
    WINDOW w_running AS (ORDER BY tier_value_percent_rank_3 DESC NULLS LAST)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        tier_value,
        tier_value_cume_dist,
        tier_value_percent_rank,
        tier_value_cume_dist_cume_dist,
        tier_value_percent_rank_2,
        tier_value_percent_rank_3,
        tier_value_cume_dist_rank,
        tier_value_percent_rank_3_dense_rank,
        tier_value_percent_rank_3_percent_rank,
        tier_value_percent_rank_3_dense_rank_percent_rank,
        tier_value_rank_dense_rank,
        tier_value_percent_rank_3_dense_rank_2,
        tier_value_percent_rank_2 * tier_value_percent_rank_3_dense_rank_percent_rank AS tier_value_percent_rank_2_times_tier_value_percent_rank_3_dense_rank_percent_rank
    FROM with_windows
    WHERE tier_value_percent_rank_3_dense_rank_2 = 8
)
SELECT
    id,
    tier_value,
    tier_value_cume_dist,
    tier_value_cume_dist_cume_dist,
    tier_value_percent_rank_3_dense_rank,
    tier_value_percent_rank_3_percent_rank,
    tier_value_percent_rank_3_dense_rank_percent_rank,
    tier_value_rank_dense_rank,
    tier_value_percent_rank_3_dense_rank_2
FROM base
ORDER BY tier_value_percent_rank_3_dense_rank DESC NULLS FIRST;
