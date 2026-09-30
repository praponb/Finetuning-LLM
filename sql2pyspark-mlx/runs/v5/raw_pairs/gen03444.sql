WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(tier_value) OVER w_hist AS plan_avg_tier_value,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value ASC) AS tier_value_dense_rank,
        MAX(tier_value) OVER (PARTITION BY currency) AS currency_max_tier_value
    FROM flattened
    WINDOW w_hist AS (PARTITION BY plan, currency)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM enriched
    WHERE effective_from >= DATE '2024-03-19'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        effective_from,
        tier_value,
        plan_avg_tier_value,
        tier_value_dense_rank,
        currency_max_tier_value,
        days_since_effective_from * currency_max_tier_value AS days_since_effective_from_times_currency_max_tier_value
    FROM cleaned
    WHERE effective_from >= DATE '2024-02-08'
      AND days_since_effective_from >= 44
),
enriched_price_tiers AS (
    -- Ranking and gap features
    SELECT
        plan,
        effective_from,
        plan_avg_tier_value,
        currency_max_tier_value,
        RANK() OVER w_ordered AS days_since_effective_from_times_currency_max_tier_value_rank,
        AVG(tier_value) OVER (PARTITION BY plan) AS plan_avg_tier_value_2
    FROM base
    WHERE plan = 'basic'
    WINDOW w_ordered AS (ORDER BY days_since_effective_from_times_currency_max_tier_value DESC NULLS LAST)
)
-- Aggregate with conditional metrics
SELECT
    plan,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(currency_max_tier_value, ARRAY(0.25, 0.5, 0.75)) AS currency_max_tier_value_quartiles
FROM enriched_price_tiers
GROUP BY plan
ORDER BY plan ASC NULLS LAST;
