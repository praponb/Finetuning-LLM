WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) x AS tier_value
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, DAY(effective_from) AS effective_from_day
    FROM exploded
    WHERE effective_from >= DATE '2024-05-19'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        tier_value,
        effective_from_day,
        COUNT(tier_value) OVER (PARTITION BY currency) AS currency_count_tier_value,
        SUM(effective_from_day) OVER (PARTITION BY currency) AS currency_sum_effective_from_day,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value NULLS LAST) AS tier_value_percent_rank
    FROM valid
    WHERE effective_from BETWEEN '2023-11-01' AND '2024-01-07'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tier_value_percent_rank DESC NULLS LAST) AS tier_value_percent_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_percent_rank DESC NULLS FIRST) AS tier_value_percent_rank_percent_rank,
        RANK() OVER w_ordered AS tier_value_rank,
        CUME_DIST() OVER w_running AS currency_sum_effective_from_day_cume_dist
    FROM scored
    WINDOW w_ordered AS (ORDER BY tier_value NULLS FIRST), w_running AS (ORDER BY currency_sum_effective_from_day NULLS LAST)
),
valid_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        tier_value,
        effective_from_day,
        currency_count_tier_value,
        tier_value_percent_rank_dense_rank,
        tier_value_rank,
        COALESCE(tier_value_percent_rank_dense_rank, 0) AS tier_value_percent_rank_dense_rank_filled,
        COALESCE(currency_sum_effective_from_day_cume_dist, 0) AS currency_sum_effective_from_day_cume_dist_filled
    FROM metrics
    WHERE effective_from BETWEEN '2024-03-01' AND '2024-08-01'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(tier_value) AS tier_value_whole
    FROM valid_price_tiers
    WHERE tier_value_percent_rank_dense_rank_filled > 22
)
SELECT
    id,
    effective_from_day,
    tier_value_rank,
    currency_sum_effective_from_day_cume_dist_filled
FROM cleaned
ORDER BY id DESC NULLS LAST, tier_value_rank DESC
