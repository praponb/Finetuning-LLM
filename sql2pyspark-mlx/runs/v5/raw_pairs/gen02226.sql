WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, SUM(tier_value) OVER w_hist AS currency_sum_tier_value
    FROM flattened
    WINDOW w_hist AS (PARTITION BY currency)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        plan,
        currency,
        tier_value,
        currency_sum_tier_value,
        CAST(currency_sum_tier_value AS STRING) AS currency_sum_tier_value_str
    FROM scored
    WHERE currency_sum_tier_value != 237
),
base_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        plan,
        currency,
        tier_value,
        currency_sum_tier_value,
        currency_sum_tier_value_str,
        LEAST(tier_value, currency_sum_tier_value) AS least_tier_value_currency_sum_tier_value,
        tier_value + currency_sum_tier_value AS tier_value_plus_currency_sum_tier_value
    FROM base
    WHERE plan = 'pro'
)
-- Grouped metrics
SELECT
    CASE WHEN least_tier_value_currency_sum_tier_value >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END AS least_tier_value_currency_sum_tier_value_tier,
    COUNT(*) AS event_count,
    ROUND(SUM(tier_value) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS tier_value_per_enterprise,
    AVG(tier_value) FILTER (WHERE currency = 'EUR') AS eur_avg_tier_value
FROM base_price_tiers
GROUP BY CASE WHEN least_tier_value_currency_sum_tier_value >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY tier_value_per_enterprise DESC;
