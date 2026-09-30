WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN val >= 90 THEN 'High' WHEN val >= 25 THEN 'Medium' ELSE 'Low' END AS val_tier
    FROM flattened
    WHERE effective_from >= DATE '2024-02-22'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(effective_from, 6) AS effective_from_plus_months
    FROM cleaned
    WHERE effective_from >= TO_DATE('2024-06-15')
),
kept_keys AS (
    -- Set intersection on keys
    SELECT currency, id
    FROM filtered
    INTERSECT ALL
    SELECT currency, id
    FROM price_tiers
    WHERE effective_from > '2024-08-07'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.currency = k.currency AND s.id = k.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        plan,
        currency,
        val
    FROM reconciled
) src
PIVOT (
    SUM(val) AS total
    FOR currency IN ('EUR', 'USD', 'THB')
);
