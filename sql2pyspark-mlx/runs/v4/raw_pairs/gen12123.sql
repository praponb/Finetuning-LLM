WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS element
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), effective_from) days_since_effective_from
    FROM lines
    WHERE element IS NULL
),
cleaned_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(days_since_effective_from, element) least_days_since_effective_from_element,
        FLOOR(days_since_effective_from) days_since_effective_from_whole
    FROM cleaned
    WHERE effective_from > DATE '2024-06-10'
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
        ROUND(AVG(days_since_effective_from_whole), 2) AS avg_days_since_effective_from_whole,
        APPROX_PERCENTILE(days_since_effective_from_whole, 0.95) AS days_since_effective_from_whole_p95,
        ROUND(AVG(least_days_since_effective_from_element), 2) AS avg_least_days_since_effective_from_element,
        APPROX_PERCENTILE(days_since_effective_from, 0.9) AS days_since_effective_from_p90,
        COUNT(*) AS row_count
    FROM cleaned_price_tiers
    GROUP BY DATE_TRUNC('QUARTER', effective_from)
)
SELECT
    quarter_start,
    days_since_effective_from_whole_p95,
    days_since_effective_from_p90
FROM grouped;
