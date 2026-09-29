WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS tier_value
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(currency, 1, 4) AS currency_prefix,
        UPPER(currency) AS currency_upper
    FROM flattened
    WHERE plan IN ('enterprise', 'starter')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        plan,
        tier_value,
        currency_upper,
        PERCENT_RANK() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_percent_rank,
        CUME_DIST() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist
    FROM filtered
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        plan,
        currency_upper,
        tier_value_percent_rank,
        tier_value_cume_dist,
        tier_value_cume_dist + tier_value_percent_rank AS tier_value_cume_dist_plus_tier_value_percent_rank,
        ROUND(tier_value_cume_dist, 1) AS tier_value_cume_dist_rounded
    FROM with_windows
    WHERE tier_value_percent_rank >= 0.57
),
stats AS (
    -- Multi-level aggregation
    SELECT
        plan,
        CASE WHEN tier_value_cume_dist_rounded >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_cume_dist_rounded_tier,
        COUNT(CASE WHEN plan = 'pro' THEN 1 END) AS pro_count,
        ROUND(AVG(tier_value_cume_dist_plus_tier_value_percent_rank), 2) AS avg_tier_value_cume_dist_plus_tier_value_percent_rank,
        ROUND(SUM(tier_value_cume_dist_plus_tier_value_percent_rank) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS tier_value_cume_dist_plus_tier_value_percent_rank_per_enterprise
    FROM base
    GROUP BY plan, tier_value_cume_dist_rounded_tier
    HAVING ROUND(SUM(tier_value_cume_dist_plus_tier_value_percent_rank) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) > 0
)
SELECT
    plan,
    tier_value_cume_dist_rounded_tier,
    pro_count,
    avg_tier_value_cume_dist_plus_tier_value_percent_rank,
    tier_value_cume_dist_plus_tier_value_percent_rank_per_enterprise
FROM stats
ORDER BY pro_count DESC;
