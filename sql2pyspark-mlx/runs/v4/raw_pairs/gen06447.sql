WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS tier_value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist
    FROM flattened
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN plan IN ('starter', 'pro') THEN 'Core' ELSE 'Other' END AS plan_group
    FROM metrics
    WHERE effective_from > DATE '2024-03-16'
),
valid_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_ADD(effective_from, 45) AS effective_from_plus_45d
    FROM valid
    WHERE plan NOT IN ('starter', 'pro', 'enterprise')
),
metrics_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(tier_value_rank / SUM(tier_value_rank) OVER (PARTITION BY plan), 4) AS tier_value_rank_share,
        AVG(tier_value) OVER w_recent AS plan_avg_tier_value,
        AVG(tier_value_rank) OVER w_recent AS plan_avg_tier_value_rank,
        COUNT(tier_value) OVER w_recent AS plan_count_tier_value
    FROM valid_price_tiers
    WINDOW w_recent AS (PARTITION BY plan)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(plan_avg_tier_value, 0) AS plan_avg_tier_value_filled
    FROM metrics_price_tiers
    WHERE plan_avg_tier_value != 62
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT plan, id
    FROM base
    INTERSECT ALL
    SELECT plan, id
    FROM price_tiers
    WHERE plan != 'starter'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.plan = k.plan AND s.id = k.id
)
SELECT
    tier_value,
    tier_value_rank,
    tier_value_cume_dist,
    plan_group,
    effective_from_plus_45d,
    tier_value_rank_share,
    plan_avg_tier_value_rank
FROM reconciled
ORDER BY tier_value;
