WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(budget * 0.9, 2) AS budget_adjusted,
        LOWER(dept_name) AS dept_name_lower,
        COALESCE(dept_name, 'NONE') AS dept_name_filled
    FROM departments
    WHERE location IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name_lower,
        AVG(budget) OVER w_hist AS running_avg_budget,
        COUNT(budget) OVER w_hist AS running_count_budget,
        MAX(budget) OVER w_hist AS running_max_budget,
        SUM(budget) OVER w_hist AS running_sum_budget,
        AVG(budget_adjusted) OVER w_grp AS budget_adjusted_avg_3
    FROM base
    WINDOW w_hist AS (PARTITION BY dept_name ORDER BY budget_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_windows_departments AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(running_max_budget) OVER (ORDER BY budget_adjusted_avg_3, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_max_budget_avg_5_prior,
        LAST_VALUE(budget_adjusted_avg_3, TRUE) OVER (ORDER BY budget_adjusted_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget_adjusted_avg_3
    FROM with_windows
    WHERE running_sum_budget <= 5574000
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(budget_adjusted_avg_3) OVER (ORDER BY running_sum_budget, id) AS next_budget_adjusted_avg_3,
        LAG(budget_adjusted_avg_3) OVER (ORDER BY running_sum_budget, id) AS prev_budget_adjusted_avg_3,
        LEAD(running_avg_budget) OVER (ORDER BY running_sum_budget, id) AS next_running_avg_budget,
        LAG(running_avg_budget) OVER (ORDER BY running_sum_budget, id) AS prev_running_avg_budget,
        ROW_NUMBER() OVER (ORDER BY running_sum_budget, id) AS row_seq,
        LEAD(budget_adjusted_avg_3, 1) OVER (ORDER BY budget_adjusted_avg_3, id) AS next_budget_adjusted_avg_3_2
    FROM with_windows_departments
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_running_avg_budget >= 3437000 THEN 'LARGE' ELSE 'SMALL' END AS next_running_avg_budget_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(running_max_budget_avg_5_prior) AS total_running_max_budget_avg_5_prior
FROM enriched
GROUP BY CASE WHEN next_running_avg_budget >= 3437000 THEN 'LARGE' ELSE 'SMALL' END
HAVING SUM(running_max_budget_avg_5_prior) > 0
