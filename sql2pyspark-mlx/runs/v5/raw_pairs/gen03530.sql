WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) t AS element
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_SUB(effective_from, 27) effective_from_minus_27d,
        LOWER(plan) plan_lower
    FROM flattened
    WHERE element > 56
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        element,
        effective_from_minus_27d,
        plan_lower,
        DENSE_RANK() OVER (ORDER BY element) element_dense_rank,
        SUM(element) OVER (PARTITION BY plan) plan_sum_element,
        RANK() OVER (ORDER BY element) element_rank,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY element DESC) element_dense_rank_2,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY element DESC) element_dense_rank_3
    FROM valid
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY element_dense_rank_2 NULLS FIRST) element_dense_rank_2_percent_rank,
        DENSE_RANK() OVER (ORDER BY element_rank DESC) element_rank_dense_rank
    FROM scored
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', effective_from_minus_27d) AS year_start,
    CASE WHEN element_dense_rank_3 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS element_dense_rank_3_tier,
    PERCENTILE_APPROX(element_dense_rank_2, ARRAY(0.25, 0.5, 0.75)) AS element_dense_rank_2_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(element_dense_rank_2) AS total_element_dense_rank_2
FROM with_windows
GROUP BY DATE_TRUNC('YEAR', effective_from_minus_27d), CASE WHEN element_dense_rank_3 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING SUM(element_dense_rank_2) > 0;
