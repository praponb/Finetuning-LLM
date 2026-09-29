WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS val
)
SELECT id, val
FROM exploded
ORDER BY val DESC NULLS FIRST, id;
