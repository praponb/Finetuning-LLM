WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS tier_value
),
with_windows AS (
    SELECT
        id,
        plan,
        MIN(tier_value) OVER w_ordered currency_min_tier_value,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value NULLS LAST) tier_value_cume_dist,
        MAX(tier_value) OVER w_ordered currency_max_tier_value,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY tier_value DESC NULLS LAST) tier_value_dense_rank
    FROM flattened
    WINDOW w_ordered AS (PARTITION BY currency)
),
kept_keys AS (
    SELECT id
    FROM with_windows
    INTERSECT
    SELECT id
    FROM price_tiers
    WHERE plan IN ('starter', 'pro', 'enterprise')
),
verified AS (
    SELECT s.*
    FROM with_windows s
    JOIN kept_keys k ON s.id = k.id
),
with_windows_price_tiers AS (
    SELECT *, COUNT(tier_value_dense_rank) OVER (PARTITION BY plan) plan_count_tier_value_dense_rank
    FROM verified
    WHERE tier_value_dense_rank = 4
),
matched_keys AS (
    SELECT plan, id
    FROM with_windows_price_tiers
    INTERSECT ALL
    SELECT plan, id
    FROM price_tiers
    WHERE effective_from < '2024-12-31'
),
reconciled AS (
    SELECT s.*
    FROM with_windows_price_tiers s
    JOIN matched_keys k ON s.plan = k.plan AND s.id = k.id
)
SELECT
    id,
    tier_value_cume_dist,
    currency_max_tier_value,
    tier_value_dense_rank,
    plan_count_tier_value_dense_rank
FROM reconciled
ORDER BY tier_value_cume_dist
