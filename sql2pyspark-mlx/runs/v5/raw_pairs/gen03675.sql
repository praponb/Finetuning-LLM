WITH exploded AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
)
SELECT id, val
FROM exploded
ORDER BY id DESC
