WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share_2,
        SUM(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
    FROM employees
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(job_title, ', ', level) AS job_title_level
    FROM scored
    WHERE manager_id = 21
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary_share_2 DESC, id) = 1
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
    s.salary_share,
    (SELECT MIN(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS min_employee_salary
FROM ranked s;
