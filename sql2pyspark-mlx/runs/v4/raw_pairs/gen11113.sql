WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS tier_value
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN tier_value >= 61 THEN 'High' WHEN tier_value >= 22 THEN 'Medium' ELSE 'Low' END AS tier_value_level
    FROM flattened
    WHERE effective_from > DATE '2024-06-03'
      AND plan IN ('enterprise', 'pro', 'starter')
),
remaining_keys AS (
    -- Set difference on keys
    SELECT plan, id
    FROM cleaned
    EXCEPT
    SELECT plan, id
    FROM price_tiers
    WHERE currency IN ('USD', 'THB', 'EUR')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.plan = k.plan AND s.id = k.id
)
SELECT
    plan,
    tier_value,
    tier_value_level
FROM reconciled;
