WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER w_running AS tier_value_percent_rank,
        RANK() OVER w_running AS tier_value_rank
    FROM exploded
    WINDOW w_running AS (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        currency,
        tier_value,
        tier_value_percent_rank * tier_value AS tier_value_percent_rank_times_tier_value
    FROM metrics
    WHERE currency = 'USD'
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN currency = 'THB' THEN 1 ELSE 0 END AS is_thb
    FROM cleaned
    WHERE tier_value_percent_rank_times_tier_value <= 61
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tier_value_percent_rank_times_tier_value NULLS LAST) AS tier_value_percent_rank_times_tier_value_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank,
        SUM(tier_value_percent_rank_times_tier_value) OVER (PARTITION BY currency) AS currency_sum_tier_value_percent_rank_times_tier_value,
        RANK() OVER (ORDER BY tier_value_percent_rank_times_tier_value DESC) AS tier_value_percent_rank_times_tier_value_rank
    FROM valid
),
daily AS (
    -- Grouped metrics
    SELECT
        currency,
        COUNT(*) AS num_rows,
        SIZE(COLLECT_SET(currency)) AS num_currencies
    FROM metrics_price_tiers
    GROUP BY currency
)
SELECT currency, num_currencies
FROM daily
ORDER BY num_currencies DESC
