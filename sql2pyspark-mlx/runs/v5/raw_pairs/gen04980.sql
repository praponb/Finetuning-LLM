WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        effective_from,
        currency,
        tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value) AS tier_value_dense_rank
    FROM flattened
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(currency) AS currency_length
    FROM with_windows
    WHERE tier_value_dense_rank = 7
),
scored AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        currency,
        tier_value,
        tier_value_dense_rank,
        currency_length,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value_dense_rank) AS tier_value_dense_rank_percent_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_dense_rank DESC) AS tier_value_dense_rank_dense_rank,
        MIN(tier_value_dense_rank) OVER (PARTITION BY currency) AS currency_min_tier_value_dense_rank
    FROM filtered
    WHERE YEAR(effective_from) = 2024
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    currency,
    APPROX_PERCENTILE(tier_value_dense_rank_percent_rank, 0.5) AS tier_value_dense_rank_percent_rank_p50,
    SIZE(COLLECT_SET(currency)) AS num_currencies
FROM scored
GROUP BY DATE_TRUNC('WEEK', effective_from), currency
ORDER BY tier_value_dense_rank_percent_rank_p50 DESC NULLS LAST
