WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS tier_value
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_dense_rank,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_rank,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY currency) AS currency_stddev_samp_tier_value
    FROM lines
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(tier_value_rank * 0.85, 2) AS tier_value_rank_adjusted,
        tier_value_rank / NULLIF(currency_stddev_samp_tier_value, 0) AS tier_value_rank_per_currency_stddev_samp_tier_value
    FROM with_windows
    WHERE effective_from > TO_DATE('2024-10-19')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY currency_stddev_samp_tier_value DESC NULLS LAST) AS currency_stddev_samp_tier_value_percent_rank,
        CUME_DIST() OVER (ORDER BY currency_stddev_samp_tier_value DESC NULLS FIRST) AS currency_stddev_samp_tier_value_cume_dist
    FROM filtered
)
SELECT
    id,
    currency,
    tier_value_dense_rank,
    tier_value_rank,
    currency_stddev_samp_tier_value,
    tier_value_rank_adjusted,
    tier_value_rank_per_currency_stddev_samp_tier_value,
    currency_stddev_samp_tier_value_percent_rank,
    currency_stddev_samp_tier_value_cume_dist,
    CASE WHEN currency RLIKE '^(USD|THB)$' THEN 'CORE' WHEN currency RLIKE '^(EUR)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS currency_zone
FROM scored
ORDER BY tier_value_rank_adjusted DESC NULLS FIRST, currency
