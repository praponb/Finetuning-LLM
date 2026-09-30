WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(tier_value, 2) AS tier_value_rounded,
        TRIM(plan) AS clean_plan
    FROM lines
    WHERE plan = 'pro'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        tier_value,
        tier_value_rounded,
        clean_plan,
        PERCENT_RANK() OVER w_hist AS tier_value_percent_rank,
        PERCENT_RANK() OVER w_ordered AS tier_value_rounded_percent_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value NULLS FIRST) AS tier_value_dense_rank
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST), w_ordered AS (ORDER BY tier_value_rounded DESC NULLS LAST)
),
matched_keys AS (
    -- Set difference on keys
    SELECT plan, currency
    FROM enriched
    EXCEPT
    SELECT plan, currency
    FROM price_tiers
    WHERE currency IN ('EUR', 'USD', 'THB')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.plan = k.plan AND s.currency = k.currency
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        currency,
        tier_value,
        tier_value_rounded,
        tier_value_percent_rank,
        tier_value_rounded_percent_rank,
        tier_value_dense_rank,
        CASE WHEN tier_value >= 67 THEN 'High' WHEN tier_value >= 14 THEN 'Medium' ELSE 'Low' END AS tier_value_tier
    FROM reconciled
    WHERE currency IN ('EUR', 'USD', 'THB')
),
cleaned_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN tier_value_rounded >= 90 THEN 'High' WHEN tier_value_rounded >= 38 THEN 'Medium' ELSE 'Low' END AS tier_value_rounded_level
    FROM filtered
    WHERE plan = 'pro'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        currency,
        tier_value,
        tier_value_rounded,
        tier_value_percent_rank,
        tier_value_rounded_percent_rank,
        tier_value_dense_rank,
        tier_value_tier,
        tier_value_rounded_level,
        RANK() OVER (ORDER BY tier_value_rounded DESC NULLS LAST) AS tier_value_rounded_rank,
        STDDEV_SAMP(tier_value_rounded) OVER (PARTITION BY plan) AS plan_stddev_samp_tier_value_rounded,
        MAX(tier_value_rounded_percent_rank) OVER (PARTITION BY plan, currency) AS plan_max_tier_value_rounded_percent_rank,
        MAX(tier_value_rounded_percent_rank) OVER (PARTITION BY currency, plan) AS currency_max_tier_value_rounded_percent_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value_rounded) AS tier_value_rounded_cume_dist
    FROM cleaned_price_tiers
)
-- Running and moving aggregates
SELECT
    *,
    CUME_DIST() OVER (PARTITION BY currency, plan ORDER BY tier_value_rounded_rank NULLS LAST) AS tier_value_rounded_rank_cume_dist,
    PERCENT_RANK() OVER (ORDER BY tier_value_rounded_cume_dist NULLS LAST) AS tier_value_rounded_cume_dist_percent_rank,
    COUNT(tier_value) OVER (PARTITION BY currency, plan) AS currency_count_tier_value
FROM scored
