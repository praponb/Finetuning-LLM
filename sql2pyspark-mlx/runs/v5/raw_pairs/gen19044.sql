WITH valid AS (
    SELECT
        *,
        LENGTH(dept_name) AS dept_name_length,
        COALESCE(location, 'N/A') AS location_filled,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_filled
    FROM departments
    WHERE location IS NULL
),
with_latest AS (
    SELECT s.*, x.employee_salary
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        id,
        dept_name,
        budget,
        location_filled,
        MAX(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_budget,
        COUNT(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_budget,
        AVG(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_budget
    FROM with_latest
),
base AS (
    SELECT
        id,
        dept_name,
        budget,
        location_filled,
        last3_count_budget,
        last3_avg_budget,
        COALESCE(last3_count_budget, 0) AS last3_count_budget_filled,
        COALESCE(dept_name, 'NONE') AS resolved_dept_name
    FROM metrics
    WHERE dept_name IS NOT NULL
      AND last3_avg_budget BETWEEN 1135000 AND 1396000
)
SELECT *
FROM (
    SELECT
        id,
        dept_name,
        last3_avg_budget,
        budget,
        last3_count_budget_filled
    FROM base
) src
UNPIVOT (
    metric_value FOR measure IN (last3_avg_budget AS LAST3_AVG_BUDGET, budget AS BUDGET_VAL, last3_count_budget_filled)
) long_rows
