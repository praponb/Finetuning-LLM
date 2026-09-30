WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element) AS element_cume_dist,
        RANK() OVER (ORDER BY element DESC NULLS FIRST) AS element_rank,
        PERCENT_RANK() OVER (ORDER BY element DESC NULLS FIRST) AS element_percent_rank
    FROM exploded
    WHERE currency IN ('THB', 'EUR')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        effective_from,
        element,
        element_cume_dist,
        element_rank,
        element_percent_rank,
        CASE WHEN element >= 80 THEN 'High' WHEN element >= 20 THEN 'Medium' ELSE 'Low' END AS element_tier
    FROM enriched
    WHERE effective_from > DATE '2024-04-24'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(element_rank AS DOUBLE) AS element_rank_str
    FROM valid
    WHERE element BETWEEN 45 AND 61
)
SELECT
    id,
    effective_from,
    element,
    element_rank,
    element_percent_rank
FROM base
ORDER BY id NULLS LAST;
