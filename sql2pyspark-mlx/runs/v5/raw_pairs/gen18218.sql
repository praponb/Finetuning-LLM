WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS tier_value
    FROM price_tiers
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN currency = 'USD' THEN 1 ELSE 0 END AS is_usd
    FROM exploded
    WHERE effective_from >= DATE '2024-01-30'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        currency,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist
    FROM filtered
    WHERE is_usd > 43
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value_cume_dist DESC NULLS LAST) AS tier_value_cume_dist_rank,
        SUM(tier_value_cume_dist) OVER w_running AS currency_sum_tier_value_cume_dist,
        DENSE_RANK() OVER w_grp AS tier_value_cume_dist_dense_rank
    FROM enriched
    WINDOW w_running AS (PARTITION BY currency), w_grp AS (ORDER BY tier_value_cume_dist DESC NULLS LAST)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(tier_value_cume_dist_dense_rank) OVER (PARTITION BY currency) AS currency_avg_tier_value_cume_dist_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY currency_sum_tier_value_cume_dist DESC NULLS LAST) AS currency_sum_tier_value_cume_dist_percent_rank
    FROM metrics
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(currency, 1, 2) AS currency_prefix
    FROM scored
    WHERE effective_from > TO_DATE('2024-03-07')
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY tier_value_cume_dist_rank NULLS FIRST) AS tier_value_cume_dist_rank_percent_rank,
        AVG(currency_avg_tier_value_cume_dist_dense_rank) OVER (PARTITION BY currency) AS currency_avg_currency_avg_tier_value_cume_dist_dense_rank,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value_cume_dist_dense_rank DESC NULLS LAST) AS tier_value_cume_dist_dense_rank_rank
    FROM base
)
SELECT
    id,
    effective_from,
    currency,
    tier_value_cume_dist,
    tier_value_cume_dist_rank,
    tier_value_cume_dist_dense_rank,
    currency_avg_tier_value_cume_dist_dense_rank,
    currency_sum_tier_value_cume_dist_percent_rank,
    currency_prefix
FROM metrics_price_tiers
ORDER BY tier_value_cume_dist DESC
