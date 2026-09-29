WITH lines AS (
    SELECT
        id,
        currency,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
)
SELECT id
FROM lines
ORDER BY id DESC
