WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
with_windows AS (
    SELECT
        *,
        SUM(tier_value) OVER (PARTITION BY plan) AS plan_sum_tier_value,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY tier_value) AS tier_value_dense_rank,
        RANK() OVER w_hist AS tier_value_rank
    FROM flattened
    WINDOW w_hist AS (PARTITION BY plan ORDER BY tier_value NULLS FIRST)
),
base AS (
    SELECT *, DAY(effective_from) AS effective_from_day
    FROM with_windows
    WHERE effective_from > DATE '2024-07-29'
      AND currency NOT IN ('USD', 'EUR')
),
valid AS (
    SELECT
        *,
        ROUND(tier_value_dense_rank, 0) AS tier_value_dense_rank_rounded,
        CEIL(tier_value_dense_rank) AS tier_value_dense_rank_whole
    FROM base
    WHERE effective_from >= DATE '2024-06-30'
),
enriched AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        plan_sum_tier_value,
        tier_value_dense_rank,
        tier_value_rank,
        effective_from_day,
        tier_value_dense_rank_rounded,
        tier_value_dense_rank_whole,
        PERCENT_RANK() OVER w_part AS tier_value_dense_rank_rounded_percent_rank,
        SUM(plan_sum_tier_value) OVER (PARTITION BY plan, currency) AS plan_sum_plan_sum_tier_value,
        PERCENT_RANK() OVER w_part_2 AS tier_value_dense_rank_rounded_percent_rank_2,
        ROUND(tier_value_dense_rank / SUM(tier_value_dense_rank) OVER (PARTITION BY plan), 4) AS tier_value_dense_rank_share,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY plan_sum_tier_value NULLS FIRST) AS plan_sum_tier_value_dense_rank
    FROM valid
    WINDOW w_part AS (ORDER BY tier_value_dense_rank_rounded), w_part_2 AS (PARTITION BY currency ORDER BY tier_value_dense_rank_rounded DESC)
),
base_price_tiers AS (
    SELECT *, CAST(tier_value AS INT) AS tier_value_int
    FROM enriched
    WHERE tier_value_dense_rank_rounded >= 49
)
SELECT
    CASE WHEN tier_value_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS tier_value_dense_rank_tier,
    COUNT(CASE WHEN plan = 'pro' THEN 1 END) AS pro_count,
    ROUND(SUM(plan_sum_tier_value) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS plan_sum_tier_value_per_usd,
    ROUND(SUM(tier_value_int) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS tier_value_int_per_starter
FROM base_price_tiers
GROUP BY ROLLUP (CASE WHEN tier_value_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY pro_count;
