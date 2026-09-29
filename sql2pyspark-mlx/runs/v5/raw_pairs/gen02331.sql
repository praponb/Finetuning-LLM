WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(effective_from) AS effective_from_month_end
    FROM flattened
    WHERE element < 24
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ADD_MONTHS(effective_from_month_end, 12) AS effective_from_month_end_plus_months
    FROM base
    WHERE effective_from BETWEEN '2023-08-01' AND '2024-08-01'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_ADD(effective_from_month_end, 59) AS effective_from_month_end_plus_59d,
        CASE WHEN element >= 73 THEN 'High' WHEN element >= 18 THEN 'Medium' ELSE 'Low' END AS element_level
    FROM cleaned
    WHERE effective_from BETWEEN '2023-10-01' AND '2024-08-19'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, RANK() OVER (ORDER BY element DESC) AS element_rank
    FROM valid
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, RANK() OVER (ORDER BY element NULLS LAST) AS element_rank_2
    FROM metrics
),
valid_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        effective_from,
        element,
        effective_from_month_end,
        effective_from_month_end_plus_months,
        effective_from_month_end_plus_59d,
        element_level,
        element_rank,
        element_rank_2,
        DAY(effective_from) AS effective_from_day,
        DATE_FORMAT(effective_from_month_end_plus_59d, 'MMM yyyy') AS effective_from_month_end_plus_59d_label
    FROM with_windows
    WHERE element BETWEEN 42 AND 65
)
-- Grouped metrics
SELECT
    CASE WHEN effective_from_day >= 62 THEN 'PREMIUM' ELSE 'STANDARD' END AS effective_from_day_tier,
    MIN(element) AS min_element,
    SUM(effective_from_day) AS total_effective_from_day,
    COUNT(*) AS row_count,
    MIN(effective_from_day) AS min_effective_from_day
FROM valid_price_tiers
GROUP BY effective_from_day_tier
HAVING SUM(effective_from_day) >= 0
ORDER BY min_element DESC NULLS LAST
