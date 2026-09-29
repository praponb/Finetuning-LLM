WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY tier_value) AS tier_value_cume_dist,
        MAX(tier_value) OVER (PARTITION BY currency) AS currency_max_tier_value
    FROM exploded
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, currency_max_tier_value * tier_value AS currency_max_tier_value_times_tier_value
    FROM scored
    WHERE tier_value_dense_rank > 4
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(currency_max_tier_value_times_tier_value * 1.07, 2) AS adjusted_currency_max_tier_value_times_tier_value
    FROM filtered
    WHERE tier_value_dense_rank > 3
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY tier_value_cume_dist DESC NULLS LAST) AS tier_value_cume_dist_cume_dist,
        DENSE_RANK() OVER (ORDER BY tier_value) AS tier_value_dense_rank_2,
        RANK() OVER (ORDER BY tier_value_cume_dist DESC NULLS FIRST) AS tier_value_cume_dist_rank
    FROM base
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tier_value_dense_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_dense_rank_tier,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows,
    MAX(tier_value_dense_rank) AS max_tier_value_dense_rank,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(currency_max_tier_value, ARRAY(0.25, 0.5, 0.75)) AS currency_max_tier_value_quartiles
FROM with_windows
GROUP BY ROLLUP (CASE WHEN tier_value_dense_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END);
