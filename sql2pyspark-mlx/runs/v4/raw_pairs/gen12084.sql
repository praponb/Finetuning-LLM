WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(full_name, 1, 4) AS full_name_prefix
    FROM employees
    WHERE manager_id IS NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(level) IGNORE NULLS OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        AVG(salary) OVER (PARTITION BY level, job_title) AS level_avg_salary,
        LAG(salary, 1) OVER (PARTITION BY level ORDER BY hire_date, id) AS prev_salary
    FROM filtered
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(salary, 1) OVER w_part AS prev_salary_2,
        LEAD(salary, 1) OVER w_ordered AS next_salary,
        MAX(level_avg_salary) OVER w_hist AS level_max_level_avg_salary,
        NTILE(3) OVER (PARTITION BY level ORDER BY salary DESC, id) AS salary_bucket
    FROM scored
    WINDOW w_part AS (ORDER BY hire_date, id), w_ordered AS (PARTITION BY dept_id ORDER BY level_avg_salary, id), w_hist AS (PARTITION BY level, job_title)
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        src.*,
        b.budget,
        b.dept_name
    FROM with_windows src
    JOIN departments b
        ON src.dept_id = b.id
),
with_dims_employees AS (
    -- Attach employees attributes
    SELECT
        s.*,
        e.level AS employee_level,
        e.salary AS employee_salary
    FROM with_dims s
    LEFT JOIN employees e
        ON s.manager_id = e.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT level
    FROM with_dims_employees
    INTERSECT
    SELECT level
    FROM employees
    WHERE manager_id IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims_employees s
    JOIN kept_keys k ON s.level = k.level
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.job_title,
    s.hire_date,
    s.is_remote,
    s.full_name_prefix,
    s.last_known_level,
    CASE WHEN job_title RLIKE '^(Manager|Analyst)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS job_title_zone,
    (SELECT MAX(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS max_employee_salary
FROM kept_rows s
ORDER BY max_employee_salary DESC NULLS FIRST;
