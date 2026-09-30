WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(tier_value) OVER w_ordered currency_count_tier_value,
        MIN(tier_value) OVER w_ordered currency_min_tier_value,
        DENSE_RANK() OVER w_grp tier_value_dense_rank
    FROM lines
    WINDOW w_ordered AS (PARTITION BY currency), w_grp AS (PARTITION BY currency ORDER BY tier_value NULLS LAST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(currency) currency_length
    FROM scored
    WHERE tier_value != 30
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(currency) currency_length_2
    FROM valid
    WHERE currency_min_tier_value = 73
)
SELECT
    id,
    tier_value,
    currency_count_tier_value,
    currency_min_tier_value,
    tier_value_dense_rank,
    currency_length,
    currency_length_2
FROM base
ORDER BY currency_min_tier_value NULLS LAST
