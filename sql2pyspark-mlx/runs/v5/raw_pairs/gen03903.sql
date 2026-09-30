WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS tier_value
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(tier_value) OVER (PARTITION BY currency) AS currency_count_tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value) AS tier_value_dense_rank,
        RANK() OVER (ORDER BY tier_value DESC) AS tier_value_rank
    FROM lines
    WHERE effective_from < '2024-06-01'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        tier_value,
        currency_count_tier_value,
        tier_value_dense_rank,
        tier_value_rank,
        ADD_MONTHS(effective_from, 12) AS effective_from_plus_months,
        CASE WHEN tier_value_rank >= 9 THEN 'High' WHEN tier_value_rank >= 3 THEN 'Medium' ELSE 'Low' END AS tier_value_rank_level
    FROM metrics
    WHERE currency = 'THB'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_part AS currency_count_tier_value_cume_dist,
        PERCENT_RANK() OVER (ORDER BY tier_value_rank) AS tier_value_rank_percent_rank,
        RANK() OVER w_recent AS tier_value_rank_2
    FROM filtered
    WINDOW w_part AS (ORDER BY currency_count_tier_value), w_recent AS (ORDER BY tier_value DESC NULLS FIRST)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY tier_value_rank_percent_rank DESC NULLS LAST) AS tier_value_rank_percent_rank_percent_rank,
        RANK() OVER (ORDER BY tier_value_rank NULLS FIRST) AS tier_value_rank_rank
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', effective_from_plus_months) AS quarter_start,
    CASE WHEN tier_value_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_dense_rank_tier,
    SUM(currency_count_tier_value_cume_dist) AS total_currency_count_tier_value_cume_dist,
    COUNT(*) AS cnt,
    COUNT(*) AS num_rows
FROM enriched
GROUP BY DATE_TRUNC('QUARTER', effective_from_plus_months), CASE WHEN tier_value_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END;
