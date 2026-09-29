WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS tier_value
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_recent AS tier_value_rank,
        PERCENT_RANK() OVER w_recent_2 AS tier_value_percent_rank
    FROM flattened
    WHERE plan = 'basic'
    WINDOW w_recent AS (PARTITION BY plan, currency ORDER BY tier_value), w_recent_2 AS (ORDER BY tier_value DESC)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        tier_value_rank,
        tier_value_percent_rank,
        LENGTH(currency) AS currency_length
    FROM with_windows
    WHERE effective_from > DATE '2024-04-18'
),
matched_keys AS (
    -- Set difference on keys
    SELECT plan, currency
    FROM base
    EXCEPT ALL
    SELECT plan, currency
    FROM price_tiers
    WHERE plan <> 'pro'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.plan = k.plan AND s.currency = k.currency
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        tier_value,
        tier_value_rank,
        tier_value_percent_rank,
        currency_length,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY currency_length NULLS FIRST) AS currency_length_percent_rank,
        CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY tier_value NULLS LAST) AS tier_value_cume_dist
    FROM reconciled
),
with_windows_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        tier_value,
        tier_value_percent_rank,
        currency_length_percent_rank,
        tier_value_cume_dist,
        PERCENT_RANK() OVER (ORDER BY currency_length NULLS FIRST) AS currency_length_percent_rank_2,
        PERCENT_RANK() OVER (ORDER BY currency_length DESC NULLS FIRST) AS currency_length_percent_rank_3
    FROM enriched
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN currency_length_percent_rank_3 >= 0.74 THEN 'PREMIUM' ELSE 'STANDARD' END AS currency_length_percent_rank_3_tier,
        PERCENTILE_APPROX(tier_value_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS tier_value_percent_rank_quartiles,
        APPROX_PERCENTILE(currency_length_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS currency_length_percent_rank_quartiles,
        APPROX_PERCENTILE(currency_length_percent_rank_2, ARRAY(0.25, 0.5, 0.75)) AS currency_length_percent_rank_2_quartiles,
        PERCENTILE_APPROX(tier_value_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS tier_value_cume_dist_quartiles
    FROM with_windows_price_tiers
    GROUP BY CASE WHEN currency_length_percent_rank_3 >= 0.74 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    currency_length_percent_rank_3_tier,
    currency_length_percent_rank_quartiles,
    tier_value_cume_dist_quartiles
FROM grouped
ORDER BY currency_length_percent_rank_3_tier NULLS FIRST;
