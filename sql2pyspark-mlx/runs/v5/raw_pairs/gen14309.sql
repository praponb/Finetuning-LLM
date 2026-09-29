WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) x AS tier_value
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(plan, 1, 3) plan_prefix
    FROM lines
    WHERE effective_from >= DATE '2024-03-03'
      AND effective_from BETWEEN '2024-02-01' AND '2024-05-01'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        effective_from,
        tier_value,
        plan_prefix,
        CASE WHEN plan = 'basic' THEN 1 ELSE 0 END is_basic
    FROM cleaned
    WHERE effective_from >= DATE '2024-04-14'
      AND effective_from BETWEEN '2025-07-01' AND '2025-10-01'
),
scored AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        tier_value,
        plan_prefix,
        SUM(is_basic) OVER w_hist plan_sum_is_basic
    FROM valid
    WHERE tier_value BETWEEN 60 AND 61
    WINDOW w_hist AS (PARTITION BY plan)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        effective_from,
        tier_value,
        plan_sum_is_basic,
        CUME_DIST() OVER (ORDER BY tier_value DESC NULLS FIRST) tier_value_cume_dist,
        RANK() OVER (ORDER BY tier_value DESC NULLS LAST) tier_value_rank,
        RANK() OVER (ORDER BY plan_sum_is_basic DESC) plan_sum_is_basic_rank,
        DENSE_RANK() OVER (ORDER BY plan_sum_is_basic DESC NULLS LAST) plan_sum_is_basic_dense_rank
    FROM scored
),
scored_price_tiers AS (
    -- Running and moving aggregates
    SELECT *, CUME_DIST() OVER (ORDER BY plan_sum_is_basic NULLS FIRST) plan_sum_is_basic_cume_dist
    FROM metrics
),
metrics_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        effective_from,
        tier_value,
        plan_sum_is_basic,
        tier_value_cume_dist,
        plan_sum_is_basic_rank,
        plan_sum_is_basic_dense_rank,
        plan_sum_is_basic_cume_dist,
        PERCENT_RANK() OVER w_part plan_sum_is_basic_dense_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY plan_sum_is_basic DESC) plan_sum_is_basic_percent_rank
    FROM scored_price_tiers
    WINDOW w_part AS (ORDER BY plan_sum_is_basic_dense_rank DESC NULLS FIRST)
)
-- Grouped metrics
SELECT
    CASE WHEN plan_sum_is_basic >= 347 THEN 'HIGH' ELSE 'LOW' END AS plan_sum_is_basic_tier,
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    CASE WHEN plan_sum_is_basic_dense_rank_percent_rank >= 0.8 THEN 'LARGE' ELSE 'SMALL' END AS plan_sum_is_basic_dense_rank_percent_rank_tier,
    ROUND(AVG(tier_value), 2) AS avg_tier_value,
    MIN(plan_sum_is_basic_dense_rank_percent_rank) AS min_plan_sum_is_basic_dense_rank_percent_rank,
    APPROX_PERCENTILE(tier_value_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS tier_value_cume_dist_quartiles,
    AVG(tier_value) AS avg_tier_value_2,
    PERCENTILE_APPROX(plan_sum_is_basic_dense_rank_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS plan_sum_is_basic_dense_rank_percent_rank_quartiles
FROM metrics_price_tiers
GROUP BY ROLLUP (CASE WHEN plan_sum_is_basic >= 347 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', effective_from), CASE WHEN plan_sum_is_basic_dense_rank_percent_rank >= 0.8 THEN 'LARGE' ELSE 'SMALL' END)
HAVING ROUND(AVG(tier_value), 2) < 1000000
