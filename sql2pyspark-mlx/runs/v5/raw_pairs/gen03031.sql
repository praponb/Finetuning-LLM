WITH lines AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS tier_value
),
filtered AS (
    SELECT
        *,
        COALESCE(tier_value, 0) tier_value_filled,
        LOWER(currency) currency_lower
    FROM lines
    WHERE effective_from >= DATE '2024-03-23'
),
remaining_keys AS (
    SELECT currency
    FROM filtered
    INTERSECT ALL
    SELECT currency
    FROM price_tiers
    WHERE plan IN ('pro', 'basic')
),
verified AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.currency = k.currency
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tier_value_filled DESC NULLS FIRST) tier_value_filled_dense_rank,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY currency) currency_stddev_samp_tier_value
    FROM verified
    WHERE MONTH(effective_from) IN (4, 5, 12)
),
filtered_price_tiers AS (
    SELECT *, CASE WHEN currency = 'THB' THEN 1 ELSE 0 END is_thb
    FROM metrics
    WHERE currency_stddev_samp_tier_value BETWEEN 44 AND 53
),
base AS (
    SELECT
        effective_from,
        currency,
        tier_value_filled,
        currency_lower,
        tier_value_filled_dense_rank,
        currency_stddev_samp_tier_value,
        is_thb,
        DATE_TRUNC('year', effective_from) effective_from_period,
        CAST(currency_stddev_samp_tier_value AS DOUBLE) currency_stddev_samp_tier_value_num
    FROM filtered_price_tiers
    WHERE effective_from > DATE '2024-03-01'
)
SELECT
    currency_lower,
    currency_stddev_samp_tier_value,
    effective_from_period,
    currency_stddev_samp_tier_value_num,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value_filled DESC NULLS LAST) tier_value_filled_percent_rank,
    DENSE_RANK() OVER (ORDER BY tier_value_filled_dense_rank DESC) tier_value_filled_dense_rank_dense_rank,
    AVG(tier_value_filled) OVER (PARTITION BY currency) currency_avg_tier_value_filled
FROM base
