WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date,
        COALESCE(level, 'DIRECT') AS resolved_level
    FROM employees
    WHERE dept_id IN (7, 27, 32, 38)
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        src.*,
        d.dept_name,
        d.location
    FROM base src
    INNER JOIN departments d
        ON src.dept_id = d.id
    WHERE d.location <> 'London'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY manager_id ORDER BY days_since_hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY salary DESC, id) AS salary_bucket,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) AS row_seq,
        FIRST_VALUE(dept_name, TRUE) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name_2,
        LEAD(salary, 1) OVER (PARTITION BY dept_id ORDER BY days_since_hire_date, id) AS next_salary
    FROM with_dims
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(job_title) AS job_title_length,
        COALESCE(level, 'UNKNOWN') AS level_filled
    FROM enriched
    WHERE manager_id IS NOT NULL
),
cleaned_employees AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, next_salary * row_seq AS next_salary_times_row_seq
    FROM cleaned
    WHERE salary_bucket >= 4
)
SELECT
    s.id,
    s.email,
    s.dept_id,
    s.manager_id,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level,
    s.days_since_hire_date,
    CASE WHEN level RLIKE '^(L4|L2)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS level_zone,
    (SELECT SUM(x.salary) FROM employees x WHERE x.manager_id = s.manager_id) AS sum_employee_salary
FROM cleaned_employees s
ORDER BY manager_id NULLS LAST;
