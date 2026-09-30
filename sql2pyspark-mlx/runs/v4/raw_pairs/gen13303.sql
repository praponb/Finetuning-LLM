WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(salary, 2) OVER (PARTITION BY dept_id ORDER BY salary ASC, id ASC) AS prev_salary,
        LAG(salary, 2) OVER (PARTITION BY manager_id, level ORDER BY salary ASC, id ASC) AS prev_salary_2,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level, manager_id), 4) AS salary_share,
        LEAD(salary, 1) OVER (ORDER BY hire_date ASC, id ASC) AS next_salary
    FROM employees
),
joined AS (
    -- Attach departments attributes
    SELECT a.*, d.dept_name
    FROM enriched a
    LEFT JOIN departments d
        ON a.dept_id = d.id
),
enriched_employees AS (
    -- Attach employees attributes
    SELECT
        s.*,
        d.email AS employee_email,
        d.salary AS employee_salary,
        d.level AS employee_level
    FROM joined s
    LEFT JOIN employees d
        ON s.manager_id = d.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        prev_salary,
        prev_salary_2,
        salary_share,
        next_salary,
        dept_name,
        employee_email,
        employee_salary,
        employee_level,
        SUM(employee_salary) OVER (PARTITION BY employee_level, manager_id ORDER BY prev_salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_employee_salary,
        NTILE(5) OVER (ORDER BY prev_salary DESC, id ASC) AS prev_salary_bucket
    FROM enriched_employees
)
SELECT
    id,
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    is_remote,
    prev_salary_2
FROM with_windows
ORDER BY dept_id ASC NULLS FIRST;
