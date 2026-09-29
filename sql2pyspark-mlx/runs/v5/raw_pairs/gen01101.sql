WITH exploded AS (
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS val
)
SELECT
    id,
    currency,
    CASE WHEN currency RLIKE '^(USD|EUR)$' THEN 'GROUP_A' WHEN currency RLIKE '^(THB)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS currency_zone
FROM exploded
ORDER BY currency, currency_zone;
