WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
),
valid AS (
    SELECT *, LOWER(plan) AS plan_lower
    FROM lines
    WHERE effective_from >= DATE '2024-06-04'
      AND element > 69
),
cleaned AS (
    SELECT
        id,
        plan,
        effective_from,
        element,
        plan_lower,
        ROUND(element * 1.15, 2) AS element_scaled
    FROM valid
    WHERE MONTH(effective_from) IN (3, 4, 12)
),
base AS (
    SELECT
        id,
        plan,
        effective_from,
        element,
        plan_lower,
        DATE_FORMAT(effective_from, 'MMM yyyy') AS effective_from_label
    FROM cleaned
    WHERE effective_from >= TO_DATE('2024-06-18')
),
base_price_tiers AS (
    SELECT
        *,
        CASE WHEN element >= 86 THEN 'High' WHEN element >= 26 THEN 'Medium' ELSE 'Low' END AS element_band,
        INITCAP(TRIM(plan)) AS plan_title
    FROM base
    WHERE effective_from >= DATE '2024-03-14'
)
SELECT
    effective_from,
    element,
    plan_lower,
    element_band
FROM base_price_tiers
ORDER BY effective_from NULLS FIRST;
