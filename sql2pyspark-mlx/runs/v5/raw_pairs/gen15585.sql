WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS element
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        effective_from,
        currency,
        element,
        FLOOR(element) AS element_whole
    FROM exploded
    WHERE effective_from > DATE '2024-03-17'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT currency, plan
    FROM filtered
    INTERSECT ALL
    SELECT currency, plan
    FROM price_tiers
    WHERE currency <> 'USD'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.currency = k.currency AND s.plan = k.plan
),
matched_keys AS (
    -- Set intersection on keys
    SELECT currency, plan
    FROM reconciled
    INTERSECT
    SELECT currency, plan
    FROM price_tiers
    WHERE currency = 'EUR'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN matched_keys k ON s.currency = k.currency AND s.plan = k.plan
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(currency) AS currency_lower
    FROM verified
    WHERE effective_from >= DATE '2024-02-26'
      AND element_whole >= 51
)
SELECT
    plan,
    effective_from,
    element,
    element_whole,
    currency_lower,
    CASE WHEN plan RLIKE '^(pro|starter)$' THEN 'GROUP_A' ELSE 'CORE' END AS plan_zone
FROM cleaned
ORDER BY effective_from NULLS LAST
