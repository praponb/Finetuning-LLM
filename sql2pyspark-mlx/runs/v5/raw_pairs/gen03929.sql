WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS tier_value
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        currency,
        tier_value,
        MAX(tier_value) OVER (PARTITION BY currency) AS currency_max_tier_value,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank
    FROM flattened
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        currency,
        RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_rank
    FROM enriched
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN tier_value_rank >= 7 THEN 'High' WHEN tier_value_rank >= 2 THEN 'Medium' ELSE 'Low' END AS tier_value_rank_band
    FROM scored
    WHERE tier_value_rank <= 3
),
cleaned_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(tier_value_rank) AS tier_value_rank_whole
    FROM cleaned
    WHERE tier_value_rank >= 5
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT currency, id
    FROM cleaned_price_tiers
    INTERSECT
    SELECT currency, id
    FROM price_tiers
    WHERE YEAR(effective_from) >= 2025
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned_price_tiers s
    JOIN remaining_keys k ON s.currency = k.currency AND s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        currency,
        tier_value_rank,
        tier_value_rank_band,
        tier_value_rank_whole,
        tier_value_rank / NULLIF(tier_value_rank_whole, 0) AS tier_value_rank_per_tier_value_rank_whole
    FROM verified
    WHERE tier_value_rank_whole >= 49
)
-- Multi-level aggregation
SELECT
    CASE WHEN tier_value_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_rank_tier,
    COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count,
    MIN(tier_value_rank_per_tier_value_rank_whole) AS min_tier_value_rank_per_tier_value_rank_whole
FROM base
GROUP BY CASE WHEN tier_value_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY tier_value_rank_tier DESC NULLS LAST
