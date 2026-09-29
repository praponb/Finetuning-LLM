WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN dept_name IN ('HR', 'Marketing') THEN 'Group A' ELSE 'Other' END AS dept_name_group
    FROM departments
    WHERE location IS NOT NULL
      AND dept_name = 'Sales'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name_group,
        COUNT(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_budget,
        AVG(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_budget,
        SUM(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_budget
    FROM base
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(last3_sum_budget, 0) AS last3_sum_budget_filled
    FROM metrics
    WHERE last3_count_budget = 13
),
base_departments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        last3_count_budget,
        last3_sum_budget,
        last3_sum_budget_filled,
        ROUND(last3_sum_budget_filled, 1) AS last3_sum_budget_filled_rounded
    FROM filtered
    WHERE last3_count_budget <= 15
)
SELECT
    id,
    last3_sum_budget,
    last3_sum_budget_filled
FROM base_departments
ORDER BY last3_sum_budget_filled NULLS FIRST;
