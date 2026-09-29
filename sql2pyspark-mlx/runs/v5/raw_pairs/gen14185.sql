WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, plan
    FROM flattened
    INTERSECT ALL
    SELECT id, plan
    FROM price_tiers
    WHERE currency NOT IN ('USD', 'EUR')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        DENSE_RANK() OVER (ORDER BY tier_value DESC) AS tier_value_dense_rank
    FROM verified
    WHERE tier_value BETWEEN 56 AND 63
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_SUB(effective_from, 58) AS effective_from_minus_58d,
        ADD_MONTHS(effective_from, 3) AS effective_from_plus_months
    FROM enriched
    WHERE effective_from >= '2024-04-01'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        tier_value_dense_rank,
        effective_from_plus_months,
        PERCENT_RANK() OVER w_ordered AS tier_value_dense_rank_percent_rank,
        PERCENT_RANK() OVER w_ordered AS tier_value_dense_rank_percent_rank_2,
        RANK() OVER w_ordered AS tier_value_dense_rank_rank,
        CUME_DIST() OVER w_ordered AS tier_value_dense_rank_cume_dist,
        RANK() OVER (ORDER BY tier_value_dense_rank NULLS LAST) AS tier_value_dense_rank_rank_2
    FROM cleaned
    WINDOW w_ordered AS (ORDER BY tier_value_dense_rank DESC NULLS LAST)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics
    EXCEPT ALL
    SELECT id
    FROM price_tiers
    WHERE plan <> 'pro'
),
verified_price_tiers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', effective_from_plus_months) AS quarter_start,
    PERCENTILE_APPROX(tier_value_dense_rank_rank_2, 0.95) AS tier_value_dense_rank_rank_2_p95,
    MAX(tier_value_dense_rank_cume_dist) AS max_tier_value_dense_rank_cume_dist,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    APPROX_PERCENTILE(tier_value_dense_rank_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS tier_value_dense_rank_percent_rank_quartiles
FROM verified_price_tiers
GROUP BY 1
ORDER BY distinct_ids_2 NULLS FIRST;
