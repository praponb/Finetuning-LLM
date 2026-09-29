WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS element
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(element * 1.1, 2) adjusted_element
    FROM flattened
    WHERE effective_from BETWEEN '2024-04-01' AND '2024-06-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT *, DENSE_RANK() OVER (ORDER BY element DESC NULLS FIRST) element_dense_rank
    FROM valid
    WHERE adjusted_element >= 44
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        effective_from,
        element,
        adjusted_element,
        element_dense_rank,
        DATE_ADD(effective_from, 31) effective_from_plus_31d
    FROM with_windows
    WHERE adjusted_element >= 67
)
SELECT
    id,
    effective_from,
    element,
    adjusted_element,
    element_dense_rank,
    effective_from_plus_31d
FROM base
ORDER BY effective_from, id;
