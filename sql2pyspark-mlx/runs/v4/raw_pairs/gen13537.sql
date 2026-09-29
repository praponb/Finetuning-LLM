WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) t AS tier_value
),
enriched AS (
    SELECT
        *,
        MIN(tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank
    FROM flattened
),
filtered AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        currency_min_tier_value,
        tier_value_percent_rank,
        currency_min_tier_value * tier_value AS currency_min_tier_value_times_tier_value,
        tier_value_percent_rank / NULLIF(tier_value, 0) AS tier_value_percent_rank_per_tier_value
    FROM enriched
    WHERE tier_value_percent_rank BETWEEN 0.23 AND 0.77
),
matched_keys AS (
    SELECT currency, id
    FROM filtered
    EXCEPT
    SELECT currency, id
    FROM price_tiers
    WHERE currency NOT IN ('EUR', 'THB')
),
verified AS (
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.currency = k.currency AND s.id = k.id
),
remaining_keys AS (
    SELECT id
    FROM verified
    EXCEPT DISTINCT
    SELECT id
    FROM price_tiers
    WHERE effective_from >= '2024-04-01'
),
kept_rows AS (
    SELECT s.*
    FROM verified s
    JOIN remaining_keys k ON s.id = k.id
),
with_windows AS (
    SELECT
        *,
        AVG(tier_value_percent_rank_per_tier_value) OVER w_recent AS plan_avg_tier_value_percent_rank_per_tier_value,
        AVG(tier_value_percent_rank_per_tier_value) OVER w_recent AS plan_avg_tier_value_percent_rank_per_tier_value_2,
        SUM(currency_min_tier_value_times_tier_value) OVER (PARTITION BY currency) AS currency_sum_currency_min_tier_value_times_tier_value
    FROM kept_rows
    WINDOW w_recent AS (PARTITION BY plan)
)
SELECT
    CASE WHEN currency_sum_currency_min_tier_value_times_tier_value >= 402 THEN 'PREMIUM' ELSE 'STANDARD' END AS currency_sum_currency_min_tier_value_times_tier_value_tier,
    plan,
    ROUND(AVG(tier_value_percent_rank_per_tier_value), 2) AS avg_tier_value_percent_rank_per_tier_value,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN plan = 'basic' THEN currency_min_tier_value_times_tier_value ELSE 0 END) AS basic_currency_min_tier_value_times_tier_value,
    MIN(tier_value_percent_rank) AS min_tier_value_percent_rank,
    SUM(CASE WHEN plan = 'starter' THEN plan_avg_tier_value_percent_rank_per_tier_value ELSE 0 END) AS starter_plan_avg_tier_value_percent_rank_per_tier_value,
    RANK() OVER (PARTITION BY CASE WHEN currency_sum_currency_min_tier_value_times_tier_value >= 402 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SUM(CASE WHEN plan = 'starter' THEN plan_avg_tier_value_percent_rank_per_tier_value ELSE 0 END) DESC) AS starter_plan_avg_tier_value_percent_rank_per_tier_value_rank
FROM with_windows
GROUP BY currency_sum_currency_min_tier_value_times_tier_value_tier, plan
HAVING COUNT(DISTINCT id) > 1
