WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS element
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(effective_from, 'yyyy-MM') AS effective_from_label
    FROM flattened
    WHERE effective_from >= TO_DATE('2024-07-17')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('YEAR', effective_from) AS effective_from_period
    FROM filtered
    WHERE effective_from >= DATE '2024-08-03'
)
SELECT
    id,
    element,
    effective_from_label,
    effective_from_period
FROM cleaned
ORDER BY effective_from_period DESC;
