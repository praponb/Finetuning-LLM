WITH exploded AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
)
SELECT
    *,
    SUM(val) OVER (PARTITION BY currency) AS currency_sum_val,
    RANK() OVER (PARTITION BY currency ORDER BY val NULLS LAST) AS val_rank
FROM exploded;
