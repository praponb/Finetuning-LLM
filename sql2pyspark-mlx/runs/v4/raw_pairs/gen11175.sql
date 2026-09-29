WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY hire_date DESC, id) AS seq_no,
        SUM(salary) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
    FROM employees
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY running_salary DESC, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
enriched AS (
    -- Attach departments attributes
    SELECT
        e.*,
        b.budget,
        b.location,
        b.dept_name
    FROM latest e
    LEFT JOIN departments b
        ON e.dept_id = b.id
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        src.*,
        d.dept_name AS department_dept_name,
        d.location AS department_location
    FROM enriched src
    LEFT JOIN departments d
        ON src.dept_id = d.id
),
enriched_employees AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(salary) OVER w_ordered AS cumulative_salary,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY salary, id) AS row_seq,
        AVG(budget) OVER (PARTITION BY dept_id ORDER BY latest_rank, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_avg_5_prior,
        SUM(salary) OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_2
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY job_title, location ORDER BY running_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_employees AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched_employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id, dept_name ORDER BY row_seq NULLS FIRST, id) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(budget AS INT) AS budget_int,
        seq_no * cumulative_salary AS seq_no_times_cumulative_salary
    FROM latest_employees
    WHERE full_name LIKE 'Al%'
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.manager_id,
    s.salary,
    s.hire_date,
    s.level,
    s.seq_no,
    s.latest_rank,
    (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS avg_employee_salary
FROM base s
ORDER BY avg_employee_salary NULLS FIRST;
