WITH lines AS (
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS val
)
SELECT
    id,
    currency,
    val
FROM lines
ORDER BY currency DESC, val;
