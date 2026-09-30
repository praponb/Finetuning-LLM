WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS val
    FROM price_tiers
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(val) OVER w_ordered AS currency_count_val,
        PERCENT_RANK() OVER (ORDER BY val DESC) AS val_percent_rank,
        MIN(val) OVER w_ordered AS currency_min_val
    FROM exploded
    WINDOW w_ordered AS (PARTITION BY currency)
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('YEAR', effective_from) AS effective_from_period
    FROM metrics
    WHERE effective_from >= DATE '2024-01-30'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        effective_from,
        currency_count_val,
        val_percent_rank,
        currency_min_val,
        COUNT(val) OVER (PARTITION BY currency ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS val_count_30d,
        RANK() OVER w_ordered AS currency_count_val_rank,
        SUM(val) OVER (PARTITION BY currency) AS currency_sum_val
    FROM base
    WINDOW w_ordered AS (PARTITION BY currency ORDER BY currency_count_val DESC)
)
SELECT val_percent_rank, currency_sum_val
FROM scored
ORDER BY currency_sum_val NULLS LAST;
