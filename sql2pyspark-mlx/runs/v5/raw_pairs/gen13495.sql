WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) x AS tier_value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        plan,
        currency,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank,
        MAX(tier_value) OVER w_part AS currency_max_tier_value
    FROM exploded
    WINDOW w_part AS (PARTITION BY currency, plan)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(currency_max_tier_value * 0.9, 2) AS currency_max_tier_value_scaled
    FROM metrics
    WHERE currency != 'USD'
),
cleaned_price_tiers AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(currency_max_tier_value_scaled, 0) AS currency_max_tier_value_scaled_filled
    FROM cleaned
    WHERE tier_value_percent_rank BETWEEN 0.51 AND 0.63
),
cleaned_price_tiers_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        plan,
        currency,
        tier_value_percent_rank,
        currency_max_tier_value,
        currency_max_tier_value_scaled,
        currency_max_tier_value_scaled_filled,
        CASE WHEN currency = 'EUR' THEN 1 ELSE 0 END AS is_eur
    FROM cleaned_price_tiers
    WHERE currency = 'USD'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        currency,
        currency_max_tier_value_scaled_filled,
        is_eur,
        MAX(is_eur) OVER (PARTITION BY plan, currency) AS plan_max_is_eur,
        PERCENT_RANK() OVER (ORDER BY is_eur NULLS LAST) AS is_eur_percent_rank
    FROM cleaned_price_tiers_2
)
SELECT
    is_eur,
    plan_max_is_eur,
    is_eur_percent_rank
FROM scored;
