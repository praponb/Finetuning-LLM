WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS val
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(currency) AS currency_lower
    FROM lines
    WHERE effective_from >= TO_DATE('2024-09-30')
      AND currency IN ('THB', 'USD', 'EUR')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        effective_from,
        currency,
        val,
        currency_lower,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from,
        COALESCE(val, 0) AS val_filled
    FROM valid
    WHERE effective_from >= DATE '2024-03-31'
      AND currency IN ('EUR', 'THB', 'USD')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        currency,
        val,
        currency_lower,
        days_since_effective_from,
        val_filled,
        SUM(days_since_effective_from) OVER (PARTITION BY currency) AS currency_sum_days_since_effective_from,
        AVG(val) OVER (PARTITION BY currency) AS currency_avg_val,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val_filled DESC) AS val_filled_dense_rank
    FROM base
),
with_windows_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_running AS currency_avg_val_cume_dist,
        SUM(days_since_effective_from) OVER (PARTITION BY currency) AS currency_sum_days_since_effective_from_2,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_filled NULLS LAST) AS val_filled_percent_rank
    FROM with_windows
    WINDOW w_running AS (ORDER BY currency_avg_val DESC NULLS LAST)
),
valid_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, val_filled + currency_sum_days_since_effective_from_2 AS val_filled_plus_currency_sum_days_since_effective_from_2
    FROM with_windows_price_tiers
    WHERE effective_from >= DATE '2024-02-11'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    PERCENTILE_APPROX(val_filled, ARRAY(0.25, 0.5, 0.75)) AS val_filled_quartiles,
    PERCENTILE_APPROX(currency_avg_val, ARRAY(0.25, 0.5, 0.75)) AS currency_avg_val_quartiles
FROM valid_price_tiers
GROUP BY 1
ORDER BY week_start NULLS LAST;
