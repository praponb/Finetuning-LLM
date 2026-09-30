WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS tier_value
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tier_value) AS tier_value_dense_rank,
        STDDEV_SAMP(tier_value) OVER w_running AS currency_stddev_samp_tier_value,
        MIN(tier_value) OVER w_running AS currency_min_tier_value,
        DENSE_RANK() OVER w_running_2 AS tier_value_dense_rank_2,
        AVG(tier_value) OVER w_running AS currency_avg_tier_value
    FROM lines
    WINDOW w_running AS (PARTITION BY currency), w_running_2 AS (ORDER BY tier_value NULLS LAST)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        currency,
        tier_value,
        tier_value_dense_rank,
        currency_stddev_samp_tier_value,
        tier_value_dense_rank_2,
        ROUND(currency_stddev_samp_tier_value, 0) AS currency_stddev_samp_tier_value_rounded
    FROM enriched
    WHERE currency_min_tier_value >= 59
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(tier_value_dense_rank, 0) AS tier_value_dense_rank_filled,
        SUBSTRING(currency, 1, 3) AS currency_prefix
    FROM filtered
    WHERE currency IN ('THB', 'EUR', 'USD')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        currency,
        currency_stddev_samp_tier_value,
        tier_value_dense_rank_2,
        tier_value_dense_rank_filled,
        currency_prefix,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist,
        MIN(tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value,
        STDDEV_SAMP(currency_stddev_samp_tier_value) OVER (PARTITION BY currency) AS currency_stddev_samp_currency_stddev_samp_tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY currency_stddev_samp_tier_value_rounded NULLS LAST) AS currency_stddev_samp_tier_value_rounded_dense_rank
    FROM cleaned
    WHERE currency_stddev_samp_tier_value <> 22
)
-- Multi-level aggregation
SELECT
    CASE WHEN tier_value_dense_rank_filled >= 80 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_dense_rank_filled_tier,
    currency,
    MIN(tier_value_cume_dist) AS min_tier_value_cume_dist,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
    COUNT(CASE WHEN currency = 'THB' THEN 1 END) AS thb_count,
    COUNT(*) AS event_count,
    MAX(currency_stddev_samp_tier_value_rounded_dense_rank) FILTER (WHERE currency = 'THB') AS thb_max_currency_stddev_samp_tier_value_rounded_dense_rank
FROM metrics
GROUP BY CASE WHEN tier_value_dense_rank_filled >= 80 THEN 'LARGE' ELSE 'SMALL' END, currency
HAVING thb_count > 1
