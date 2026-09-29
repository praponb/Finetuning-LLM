WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(effective_from, 3) effective_from_plus_months
    FROM lines
    WHERE effective_from > TO_DATE('2024-04-30')
      AND val >= 27
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY val) val_dense_rank,
        SUM(val) OVER (PARTITION BY currency) currency_sum_val
    FROM valid
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, currency_sum_val - val_dense_rank currency_sum_val_minus_val_dense_rank
    FROM metrics
    WHERE effective_from > DATE '2024-04-22'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        currency,
        val,
        val_dense_rank,
        currency_sum_val,
        currency_sum_val_minus_val_dense_rank,
        FLOOR(val) val_whole
    FROM filtered
    WHERE currency = 'USD'
)
SELECT
    id,
    currency,
    val,
    currency_sum_val,
    currency_sum_val_minus_val_dense_rank,
    val_whole,
    CASE WHEN currency RLIKE '^(THB|EUR)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS currency_zone
FROM cleaned
ORDER BY val DESC NULLS FIRST;
