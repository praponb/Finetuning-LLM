WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS tier_value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_ADD(effective_from, 17) AS effective_from_plus_17d,
        DATE_TRUNC('WEEK', effective_from) AS effective_from_period
    FROM lines
    WHERE currency IN ('THB', 'EUR')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        effective_from_plus_17d,
        AVG(tier_value) OVER (ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS tier_value_avg_90d,
        COUNT(tier_value) OVER (PARTITION BY currency ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS tier_value_count_7d,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_cume_dist,
        AVG(tier_value) OVER (PARTITION BY currency ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS tier_value_avg_90d_2
    FROM filtered
    WHERE effective_from_plus_17d <= '2024-07-25'
),
filtered_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(effective_from) AS effective_from_month_end
    FROM metrics
    WHERE tier_value_avg_90d >= 62
)
SELECT
    id,
    effective_from_plus_17d,
    tier_value_avg_90d,
    tier_value_count_7d,
    tier_value_avg_90d_2
FROM filtered_price_tiers
ORDER BY effective_from_plus_17d NULLS FIRST, tier_value_count_7d;
