WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) x AS element
),
valid AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element,
        LENGTH(plan) AS plan_length
    FROM flattened
    WHERE element >= 80
),
cleaned AS (
    SELECT
        id,
        plan,
        effective_from,
        plan_length,
        LOWER(plan) AS plan_lower
    FROM valid
    WHERE currency = 'THB'
),
metrics AS (
    SELECT
        *,
        STDDEV_SAMP(plan_length) OVER (PARTITION BY plan) AS plan_stddev_samp_plan_length,
        RANK() OVER (PARTITION BY plan ORDER BY plan_length) AS plan_length_rank,
        CUME_DIST() OVER (ORDER BY plan_length) AS plan_length_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY plan_length NULLS LAST) AS plan_length_rank_2,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_length NULLS LAST) AS plan_length_cume_dist_2
    FROM cleaned
),
filtered AS (
    SELECT
        *,
        CEIL(plan_length) AS plan_length_whole,
        plan_length + plan_length_cume_dist AS plan_length_plus_plan_length_cume_dist
    FROM metrics
    WHERE effective_from > DATE '2024-02-27'
      AND plan_stddev_samp_plan_length = 67
)
SELECT
    plan,
    effective_from,
    plan_stddev_samp_plan_length,
    plan_length_cume_dist,
    plan_length_rank_2,
    plan_length_cume_dist_2,
    PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_stddev_samp_plan_length) AS plan_stddev_samp_plan_length_percent_rank,
    STDDEV_SAMP(plan_length_rank_2) OVER (PARTITION BY plan) AS plan_stddev_samp_plan_length_rank_2,
    MAX(plan_length_rank) OVER (PARTITION BY plan) AS plan_max_plan_length_rank,
    CUME_DIST() OVER (ORDER BY plan_stddev_samp_plan_length DESC) AS plan_stddev_samp_plan_length_cume_dist,
    SUM(plan_length_plus_plan_length_cume_dist) OVER (PARTITION BY plan) AS plan_sum_plan_length_plus_plan_length_cume_dist
FROM filtered
WHERE plan_stddev_samp_plan_length < 36
