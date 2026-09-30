WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(dept_name, 1, 2) AS dept_name_prefix,
        CAST(budget AS INT) AS budget_int,
        COALESCE(location, 'OTHER') AS location_filled
    FROM departments
    WHERE dept_name IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        budget * budget_int AS budget_times_budget_int,
        ROUND(budget_int, 2) AS budget_int_rounded,
        COALESCE(location, 'OTHER') AS resolved_location
    FROM filtered
    WHERE dept_name != 'Finance'
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.budget,
        s.budget_int,
        s.location_filled,
        s.budget_times_budget_int,
        s.budget_int_rounded,
        s.resolved_location,
        x.hire_date
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.employee_salary
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget ASC NULLS LAST, id ASC) AS latest_rank
    FROM latest_match
    QUALIFY latest_rank = 1
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        s.budget_int,
        s.resolved_location,
        s.employee_salary,
        s.latest_rank,
        top1.employee_hire_date
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.employee_hire_date_2
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date_2
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', employee_hire_date) AS activity_month,
    PERCENTILE_APPROX(employee_salary, 0.95) AS employee_salary_p95,
    ROUND(AVG(budget_int), 2) AS avg_budget_int,
    SUM(employee_salary) AS total_employee_salary
FROM latest_match_departments
GROUP BY DATE_TRUNC('MONTH', employee_hire_date)
ORDER BY avg_budget_int DESC
