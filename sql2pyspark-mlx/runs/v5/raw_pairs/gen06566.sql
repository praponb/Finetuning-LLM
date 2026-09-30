WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) e AS tier_value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(plan)) plan_title
    FROM lines
    WHERE currency = 'USD'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        tier_value,
        plan_title,
        LENGTH(plan) plan_length
    FROM cleaned
    WHERE tier_value <= 76
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) tier_value_percent_rank,
        RANK() OVER (ORDER BY plan_length DESC) plan_length_rank,
        COUNT(tier_value) OVER (PARTITION BY plan) plan_count_tier_value,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_length DESC) plan_length_percent_rank,
        COUNT(tier_value) OVER (PARTITION BY plan) plan_count_tier_value_2
    FROM base
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN plan_length >= 73 THEN 'LARGE' ELSE 'SMALL' END AS plan_length_tier,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    COUNT_IF(plan = 'pro') AS pro_events,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count
FROM with_windows
GROUP BY CASE WHEN plan_length >= 73 THEN 'LARGE' ELSE 'SMALL' END;
