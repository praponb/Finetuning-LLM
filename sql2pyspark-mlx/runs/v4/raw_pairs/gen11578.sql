WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS tier_value
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(tier_value AS STRING) AS tier_value_num
    FROM exploded
    WHERE currency IN ('EUR', 'USD')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        currency,
        tier_value,
        tier_value_num,
        LOWER(currency) AS currency_lower,
        LOWER(currency) AS currency_lower_2
    FROM base
    WHERE effective_from > DATE '2024-08-08'
      AND effective_from >= '2023-08-01'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        currency,
        tier_value,
        tier_value_num,
        currency_lower,
        currency_lower_2,
        PERCENT_RANK() OVER (ORDER BY tier_value ASC) AS tier_value_percent_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank
    FROM cleaned
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank_2,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value ASC NULLS FIRST) AS tier_value_rank
    FROM scored
),
scored_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(tier_value) OVER w_running AS currency_max_tier_value,
        COUNT(tier_value_percent_rank) OVER w_running AS currency_count_tier_value_percent_rank,
        STDDEV_SAMP(tier_value_percent_rank) OVER w_running AS currency_stddev_samp_tier_value_percent_rank,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value_percent_rank ASC NULLS LAST) AS tier_value_percent_rank_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value_dense_rank DESC NULLS FIRST) AS tier_value_dense_rank_percent_rank
    FROM with_windows
    WINDOW w_running AS (PARTITION BY currency)
),
totals AS (
    -- Grouped metrics
    SELECT
        currency,
        CASE WHEN tier_value_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_rank_tier,
        SIZE(COLLECT_SET(currency)) AS num_currencies,
        ROUND(SUM(currency_max_tier_value) / NULLIF(COUNT(CASE WHEN currency = 'EUR' THEN 1 END), 0), 2) AS currency_max_tier_value_per_eur,
        COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count,
        APPROX_PERCENTILE(currency_count_tier_value_percent_rank, 0.9) AS currency_count_tier_value_percent_rank_p90,
        COUNT(CASE WHEN currency = 'USD' THEN 1 END) AS usd_count
    FROM scored_price_tiers
    GROUP BY currency, CASE WHEN tier_value_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING currency_count_tier_value_percent_rank_p90 < 0
)
SELECT
    currency,
    tier_value_rank_tier,
    num_currencies,
    eur_count,
    currency_count_tier_value_percent_rank_p90
FROM totals
ORDER BY currency ASC
