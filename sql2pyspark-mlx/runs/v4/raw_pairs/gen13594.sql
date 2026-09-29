WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) t AS tier_value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value NULLS LAST) tier_value_dense_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value NULLS LAST) tier_value_cume_dist
    FROM flattened
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        tier_value_dense_rank,
        tier_value_cume_dist,
        CUME_DIST() OVER w_part tier_value_cume_dist_cume_dist,
        ROUND(tier_value_dense_rank / SUM(tier_value_dense_rank) OVER (PARTITION BY plan), 4) tier_value_dense_rank_share,
        STDDEV_SAMP(tier_value_dense_rank) OVER w_grp currency_stddev_samp_tier_value_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value_dense_rank DESC) tier_value_dense_rank_percent_rank,
        DENSE_RANK() OVER w_grp_2 tier_value_dense_rank_dense_rank
    FROM scored
    WINDOW w_part AS (PARTITION BY currency, plan ORDER BY tier_value_cume_dist DESC), w_grp AS (PARTITION BY currency), w_grp_2 AS (PARTITION BY plan ORDER BY tier_value_dense_rank DESC)
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(currency_stddev_samp_tier_value_dense_rank / SUM(currency_stddev_samp_tier_value_dense_rank) OVER (PARTITION BY plan, currency), 4) currency_stddev_samp_tier_value_dense_rank_share,
        COUNT(tier_value) OVER (PARTITION BY currency) currency_count_tier_value,
        ROUND(tier_value_dense_rank_dense_rank / SUM(tier_value_dense_rank_dense_rank) OVER (PARTITION BY currency, plan), 4) tier_value_dense_rank_dense_rank_share
    FROM metrics
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(tier_value_dense_rank_share, tier_value_cume_dist_cume_dist) greatest_tier_value_dense_rank_share_tier_value_cume_dist_cume_dist,
        LENGTH(plan) plan_length
    FROM metrics_price_tiers
    WHERE effective_from >= DATE '2024-02-17'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(tier_value_dense_rank_percent_rank AS INT) tier_value_dense_rank_percent_rank_int,
        CASE WHEN tier_value_cume_dist_cume_dist >= 0.63 THEN 'High' WHEN tier_value_cume_dist_cume_dist >= 0.39 THEN 'Medium' ELSE 'Low' END tier_value_cume_dist_cume_dist_tier
    FROM filtered
    WHERE effective_from >= DATE '2024-04-13'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_running tier_value_dense_rank_share_cume_dist,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value_dense_rank_dense_rank_share NULLS FIRST) tier_value_dense_rank_dense_rank_share_cume_dist
    FROM cleaned
    WINDOW w_running AS (ORDER BY tier_value_dense_rank_share DESC)
)
SELECT
    id,
    plan,
    tier_value_cume_dist,
    tier_value_cume_dist_cume_dist,
    currency_stddev_samp_tier_value_dense_rank_share,
    currency_count_tier_value,
    tier_value_dense_rank_dense_rank_share,
    greatest_tier_value_dense_rank_share_tier_value_cume_dist_cume_dist,
    tier_value_dense_rank_percent_rank_int
FROM enriched
ORDER BY tier_value_cume_dist DESC;
