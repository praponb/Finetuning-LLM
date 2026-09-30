WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS tier_value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(currency) AS currency_lower
    FROM flattened
    WHERE effective_from >= TO_DATE('2024-04-30')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        currency,
        tier_value,
        currency_lower,
        RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_rank,
        MAX(tier_value) OVER (PARTITION BY currency) AS currency_max_tier_value,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value NULLS LAST) AS tier_value_percent_rank
    FROM cleaned
)
-- Running and moving aggregates
SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank_2,
    RANK() OVER (ORDER BY tier_value_percent_rank DESC) AS tier_value_percent_rank_rank
FROM metrics
