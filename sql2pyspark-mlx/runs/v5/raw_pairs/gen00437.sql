WITH exploded AS (
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
    currency
FROM exploded
ORDER BY id DESC
