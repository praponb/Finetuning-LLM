WITH with_dims AS (
    -- Attach employees attributes
    SELECT
        src.*,
        d.email AS employee_email,
        d.full_name AS employee_full_name
    FROM employees src
    LEFT JOIN employees d
        ON src.manager_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(salary) OVER w_hist level_max_salary,
        LEAD(salary, 1) OVER (PARTITION BY manager_id ORDER BY salary, id) next_salary
    FROM with_dims
    WINDOW w_hist AS (PARTITION BY level)
),
joined AS (
    -- Attach departments attributes
    SELECT
        src.*,
        b.dept_name,
        b.location,
        b.budget
    FROM with_windows src
    LEFT JOIN departments b
        ON src.dept_id = b.id
),
with_dims_employees AS (
    -- Attach employees attributes
    SELECT e.*, b.job_title AS employee_job_title
    FROM joined e
    LEFT JOIN employees b
        ON e.manager_id = b.id
)
SELECT
    id,
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote
FROM with_dims_employees;
