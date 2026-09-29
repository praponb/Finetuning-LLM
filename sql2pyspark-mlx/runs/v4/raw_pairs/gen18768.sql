WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN element >= 65 THEN 'High' WHEN element >= 32 THEN 'Medium' ELSE 'Low' END AS element_tier
    FROM lines
    WHERE effective_from > DATE '2024-02-06'
      AND element <= 59
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN element >= 69 THEN 'High' WHEN element >= 20 THEN 'Medium' ELSE 'Low' END AS element_band
    FROM valid
    WHERE element >= 74
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
        DATE_TRUNC('YEAR', effective_from) AS year_start,
        SUM(element) AS total_element,
        COUNT(*) AS cnt
    FROM base
    GROUP BY DATE_TRUNC('QUARTER', effective_from), DATE_TRUNC('YEAR', effective_from)
    HAVING total_element >= 0
)
SELECT quarter_start, year_start
FROM stats
ORDER BY year_start DESC;
