WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS val
)
-- Ranking and gap features
SELECT
    id,
    plan,
    CUME_DIST() OVER (PARTITION BY currency, plan ORDER BY val DESC NULLS FIRST) AS val_cume_dist,
    PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) AS val_percent_rank,
    DENSE_RANK() OVER (PARTITION BY plan ORDER BY val DESC) AS val_dense_rank
FROM exploded
WHERE currency IN ('USD', 'THB', 'EUR');
