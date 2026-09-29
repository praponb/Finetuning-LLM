WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS element
    FROM price_tiers
)
SELECT
    id,
    effective_from,
    currency,
    element
FROM lines;
