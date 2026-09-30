WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(level, 1, 4) level_prefix,
        TRIM(full_name) clean_full_name
    FROM employees
    WHERE salary < 130000
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) salary_share,
        SUM(salary) OVER (PARTITION BY level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) salary_share_2,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) salary_share_3,
        LAG(salary, 1, 0) OVER (PARTITION BY dept_id ORDER BY hire_date, id) prev_salary
    FROM cleaned
    WHERE salary >= 98000
),
remaining_keys AS (
    -- Set difference on keys
    SELECT dept_id, manager_id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT dept_id, manager_id
    FROM employees
    WHERE level IN ('L2', 'L4')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.dept_id = k.dept_id AND s.manager_id = k.manager_id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT dept_id
    FROM verified
    INTERSECT ALL
    SELECT dept_id
    FROM employees
    WHERE is_remote = FALSE
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN kept_keys k ON s.dept_id = k.dept_id
),
joined AS (
    -- Attach employees attributes
    SELECT e.*, b.job_title AS employee_job_title
    FROM reconciled e
    LEFT JOIN employees b
        ON e.manager_id = b.id
)
-- Join a dimension and aggregate
SELECT
    e.job_title AS employee_job_title,
    e.level AS employee_level,
    COUNT(*) AS rows_total,
    MAX(a.salary) AS max_salary,
    ROUND(AVG(a.salary_share_3), 2) AS avg_salary_share_3,
    GROUPING_ID() AS gid
FROM joined a
INNER JOIN employees e ON a.manager_id = e.id
GROUP BY GROUPING SETS ((e.job_title, e.level), (e.job_title))
ORDER BY max_salary;
