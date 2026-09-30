WITH with_dims AS (
    SELECT
        src.*,
        d.salary AS employee_salary,
        d.hire_date AS employee_hire_date,
        d.level AS employee_level
    FROM employees src
    LEFT JOIN employees d
        ON src.manager_id = d.id
),
with_dims_employees AS (
    SELECT
        e.*,
        b.email AS employee_email,
        b.salary AS employee_salary_2
    FROM with_dims e
    JOIN employees b
        ON e.manager_id = b.id
),
with_dims_employees_2 AS (
    SELECT
        s.*,
        d.location,
        d.dept_name
    FROM with_dims_employees s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
with_dims_employees_3 AS (
    SELECT s.*, p.budget
    FROM with_dims_employees_2 s
    LEFT JOIN departments p
        ON s.dept_id = p.id
),
with_windows AS (
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
        employee_salary,
        employee_hire_date,
        employee_level,
        employee_email,
        employee_salary_2,
        location,
        dept_name,
        budget,
        SUM(salary) OVER (PARTITION BY job_title, level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        AVG(employee_salary_2) OVER (ORDER BY hire_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS employee_salary_2_avg_6
    FROM with_dims_employees_3
),
metrics AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY location, dept_id ORDER BY employee_salary, id) AS employee_salary_bucket,
        SUM(budget) OVER (PARTITION BY level ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        ROW_NUMBER() OVER (ORDER BY hire_date, id) AS event_seq
    FROM with_windows
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.is_remote,
    CASE WHEN full_name RLIKE '^(Jack Taylor|Alice Wong)$' THEN 'OTHER_ZONE' WHEN full_name RLIKE '^(bob smith|  Carol Diaz)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS full_name_zone,
    (SELECT MAX(x.salary) FROM employees x WHERE x.dept_id = s.dept_id) AS max_employee_salary
FROM metrics s
