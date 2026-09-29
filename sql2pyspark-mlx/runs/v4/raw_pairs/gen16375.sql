WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(tier_value * 1.1, 2) AS adjusted_tier_value
    FROM flattened
    WHERE tier_value < 41
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        tier_value,
        adjusted_tier_value,
        PERCENT_RANK() OVER (ORDER BY tier_value) AS tier_value_percent_rank,
        PERCENT_RANK() OVER (ORDER BY adjusted_tier_value DESC NULLS FIRST) AS adjusted_tier_value_percent_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank_2
    FROM cleaned
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(tier_value AS INT) AS tier_value_int,
        adjusted_tier_value / NULLIF(tier_value_percent_rank, 0) AS adjusted_tier_value_per_tier_value_percent_rank
    FROM enriched
    WHERE tier_value_percent_rank_2 > 0.74
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, adjusted_tier_value - adjusted_tier_value_per_tier_value_percent_rank AS adjusted_tier_value_minus_adjusted_tier_value_per_tier_value_percent_rank
    FROM filtered
    WHERE adjusted_tier_value_percent_rank > 0.56
),
cleaned_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        tier_value,
        tier_value_percent_rank,
        adjusted_tier_value_percent_rank,
        tier_value_percent_rank_2,
        tier_value_int,
        adjusted_tier_value_per_tier_value_percent_rank,
        adjusted_tier_value_minus_adjusted_tier_value_per_tier_value_percent_rank,
        ROUND(adjusted_tier_value_percent_rank, 0) AS adjusted_tier_value_percent_rank_rounded
    FROM valid
    WHERE adjusted_tier_value IS NOT NULL
),
enriched_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER w_grp AS adjusted_tier_value_percent_rank_dense_rank,
        RANK() OVER (ORDER BY adjusted_tier_value_per_tier_value_percent_rank DESC NULLS LAST) AS adjusted_tier_value_per_tier_value_percent_rank_rank,
        DENSE_RANK() OVER w_part AS adjusted_tier_value_per_tier_value_percent_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY adjusted_tier_value_per_tier_value_percent_rank DESC NULLS FIRST) AS adjusted_tier_value_per_tier_value_percent_rank_percent_rank
    FROM cleaned_price_tiers
    WINDOW w_grp AS (ORDER BY adjusted_tier_value_percent_rank), w_part AS (ORDER BY adjusted_tier_value_per_tier_value_percent_rank DESC)
)
SELECT
    id,
    tier_value,
    tier_value_percent_rank,
    adjusted_tier_value_percent_rank,
    tier_value_percent_rank_2,
    tier_value_int,
    adjusted_tier_value_per_tier_value_percent_rank,
    adjusted_tier_value_minus_adjusted_tier_value_per_tier_value_percent_rank,
    adjusted_tier_value_percent_rank_rounded
FROM enriched_price_tiers
