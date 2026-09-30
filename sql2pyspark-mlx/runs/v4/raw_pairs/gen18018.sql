WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN level = 'L1' THEN 1 ELSE 0 END AS is_l1
    FROM employees
    WHERE hire_date < '2023-04-01'
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
        is_l1,
        LEAD(is_l1, 1) OVER (PARTITION BY manager_id ORDER BY is_l1, id) AS next_is_l1,
        MAX(salary) OVER (PARTITION BY manager_id, level) AS manager_id_max_salary
    FROM valid
),
joined AS (
    -- Attach departments attributes
    SELECT
        a.*,
        p.location,
        p.dept_name,
        p.budget
    FROM with_windows a
    LEFT JOIN departments p
        ON a.dept_id = p.id
),
joined_employees AS (
    -- Attach employees attributes
    SELECT src.*, e.job_title AS employee_job_title
    FROM joined src
    LEFT JOIN employees e
        ON src.manager_id = e.id
)
-- Ranking and gap features
SELECT
    *,
    SUM(manager_id_max_salary) OVER w_ordered AS cumulative_manager_id_max_salary,
    PERCENT_RANK() OVER w_running AS budget_percent_rank
FROM joined_employees
WINDOW w_ordered AS (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY employee_job_title, manager_id ORDER BY budget NULLS LAST);
