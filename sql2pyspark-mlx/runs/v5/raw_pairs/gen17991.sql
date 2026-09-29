WITH ranked AS (
    SELECT
        id,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote
    FROM employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) = 1
),
enriched AS (
    SELECT
        s.id,
        s.dept_id,
        s.hire_date,
        d.budget,
        d.location
    FROM ranked s
    LEFT JOIN departments d
        ON s.dept_id = d.id
    WHERE d.dept_name IN ('Legal', 'Sales', 'Marketing')
),
latest AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date DESC, id) = 1
),
enriched_employees AS (
    SELECT
        *,
        LEAD(budget, 1) OVER (PARTITION BY dept_id, location ORDER BY hire_date, id) AS next_budget,
        SUM(budget) OVER w_running AS running_budget
    FROM latest
    WINDOW w_running AS (PARTITION BY location, dept_id ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    location,
    measure,
    metric_value
FROM (
    SELECT
        id,
        location,
        next_budget,
        budget
    FROM enriched_employees
) src
UNPIVOT (
    metric_value FOR measure IN (next_budget, budget)
);
