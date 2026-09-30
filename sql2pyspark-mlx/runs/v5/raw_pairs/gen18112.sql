WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(budget, 2) AS budget_rounded
    FROM departments
    WHERE budget > 1561000
),
filtered_departments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_name,
        location,
        budget_rounded,
        CASE WHEN budget_rounded >= 79 THEN 'High' WHEN budget_rounded >= 29 THEN 'Medium' ELSE 'Low' END AS budget_rounded_band,
        ROUND(budget_rounded * 0.85, 2) AS budget_rounded_scaled,
        COALESCE(dept_name, 'DIRECT') AS resolved_dept_name
    FROM filtered
    WHERE dept_name IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(budget_rounded_scaled) OVER (PARTITION BY location, dept_name ORDER BY budget_rounded_scaled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_budget_rounded_scaled,
        AVG(budget_rounded_scaled) OVER (PARTITION BY location, dept_name ORDER BY budget_rounded_scaled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_budget_rounded_scaled,
        SUM(budget_rounded_scaled) OVER (PARTITION BY location, dept_name ORDER BY budget_rounded_scaled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_budget_rounded_scaled,
        SUM(budget_rounded) OVER (ORDER BY budget_rounded_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget_rounded
    FROM filtered_departments
),
filtered_departments_2 AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_name,
        budget_rounded,
        budget_rounded_band,
        budget_rounded_scaled,
        resolved_dept_name,
        last3_min_budget_rounded_scaled,
        last3_avg_budget_rounded_scaled,
        last3_sum_budget_rounded_scaled,
        running_budget_rounded,
        ROUND(last3_avg_budget_rounded_scaled * 1.1, 2) AS last3_avg_budget_rounded_scaled_scaled
    FROM metrics
    WHERE dept_name IN ('HR', 'Marketing', 'Legal')
)
SELECT
    id,
    budget_rounded,
    budget_rounded_band,
    budget_rounded_scaled,
    resolved_dept_name,
    last3_sum_budget_rounded_scaled,
    running_budget_rounded,
    last3_avg_budget_rounded_scaled_scaled
FROM filtered_departments_2
ORDER BY budget_rounded DESC;
