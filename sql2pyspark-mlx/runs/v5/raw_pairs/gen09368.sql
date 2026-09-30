WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE_OUTER(discounts) AS tier_value
    FROM price_tiers
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(currency) AS currency_lower,
        CASE WHEN currency IN ('USD', 'THB') THEN 'Core' ELSE 'Other' END AS currency_group
    FROM lines
    WHERE currency IN ('EUR', 'THB', 'USD')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        currency_lower,
        currency_group,
        SUM(tier_value) OVER w_part AS currency_sum_tier_value,
        SUM(tier_value) OVER w_part AS currency_sum_tier_value_2,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value NULLS FIRST) AS tier_value_percent_rank
    FROM base
    WINDOW w_part AS (PARTITION BY currency)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, currency_sum_tier_value - currency_sum_tier_value_2 AS currency_sum_tier_value_minus_currency_sum_tier_value_2
    FROM metrics
    WHERE tier_value_percent_rank BETWEEN 0.27 AND 0.59
),
metrics_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY currency_sum_tier_value_2 NULLS FIRST) AS currency_sum_tier_value_2_dense_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_percent_rank NULLS LAST) AS tier_value_percent_rank_percent_rank
    FROM cleaned
)
SELECT
    id,
    currency_lower,
    currency_sum_tier_value,
    currency_sum_tier_value_2,
    tier_value_percent_rank,
    currency_sum_tier_value_2_dense_rank,
    tier_value_percent_rank_percent_rank
FROM metrics_price_tiers;
