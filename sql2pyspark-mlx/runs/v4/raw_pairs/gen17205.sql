WITH lines AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS tier_value
)
SELECT id, effective_from
FROM lines
ORDER BY id DESC NULLS LAST;
