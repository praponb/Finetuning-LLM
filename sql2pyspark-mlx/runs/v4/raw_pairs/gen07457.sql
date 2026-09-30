WITH lines AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
)
SELECT
    id,
    effective_from,
    currency,
    val,
    MAX(val) OVER (PARTITION BY currency) AS currency_max_val,
    MIN(val) OVER (PARTITION BY currency) AS currency_min_val,
    SUM(val) OVER (PARTITION BY currency) AS currency_sum_val
FROM lines
