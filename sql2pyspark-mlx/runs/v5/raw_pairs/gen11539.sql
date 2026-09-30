WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS element
),
scored AS (
    -- Running and moving aggregates
    SELECT *, PERCENT_RANK() OVER (ORDER BY element) element_percent_rank
    FROM lines
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        element / NULLIF(element_percent_rank, 0) element_per_element_percent_rank,
        SUBSTRING(currency, 1, 2) currency_prefix
    FROM scored
    WHERE element_percent_rank > 0.7
),
base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(element, 0) element_filled
    FROM cleaned
    WHERE currency = 'EUR'
)
-- Multi-level aggregation
SELECT
    CASE WHEN element_per_element_percent_rank >= 82 THEN 'LARGE' ELSE 'SMALL' END AS element_per_element_percent_rank_tier,
    currency,
    PERCENTILE_APPROX(element_filled, ARRAY(0.25, 0.5, 0.75)) AS element_filled_quartiles,
    PERCENTILE_APPROX(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles
FROM base
GROUP BY CASE WHEN element_per_element_percent_rank >= 82 THEN 'LARGE' ELSE 'SMALL' END, currency;
