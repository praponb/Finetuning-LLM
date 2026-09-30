WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) x AS val
)
-- Ranking and gap features
SELECT
    *,
    STDDEV_SAMP(val) OVER (PARTITION BY currency) currency_stddev_samp_val,
    MIN(val) OVER (PARTITION BY plan, currency) plan_min_val
FROM lines
