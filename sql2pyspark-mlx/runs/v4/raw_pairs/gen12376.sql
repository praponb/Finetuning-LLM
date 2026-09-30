WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS val
)
SELECT
    id,
    plan,
    effective_from,
    currency,
    val,
    CASE WHEN currency RLIKE '^(THB|EUR)$' THEN 'OTHER_ZONE' WHEN currency RLIKE '^(USD)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS currency_zone
FROM exploded
ORDER BY plan DESC NULLS LAST;
