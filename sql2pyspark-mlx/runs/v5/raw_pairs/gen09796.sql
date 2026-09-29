WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
)
SELECT
    id,
    currency,
    val,
    CASE WHEN currency RLIKE '^(EUR|USD)$' THEN 'PRIORITY' WHEN currency RLIKE '^(THB)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS currency_zone
FROM lines
ORDER BY id;
