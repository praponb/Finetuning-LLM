WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
)
SELECT currency
FROM lines
ORDER BY currency;
