WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    id,
    currency,
    val,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS FIRST) AS val_percent_rank,
    MAX(val) OVER w_part AS currency_max_val,
    SUM(val) OVER w_part AS currency_sum_val
FROM flattened
WINDOW w_part AS (PARTITION BY currency)
