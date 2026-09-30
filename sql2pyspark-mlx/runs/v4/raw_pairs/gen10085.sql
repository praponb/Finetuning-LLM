WITH cleaned AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        COALESCE(level, 'Unknown') AS level_filled,
        ROUND(salary * 0.85, 2) AS salary_scaled,
        COALESCE(job_title, 'NONE') AS resolved_job_title
    FROM employees
    WHERE hire_date >= DATE '2023-04-23'
      AND level IS NOT NULL
),
matched_keys AS (
    SELECT manager_id
    FROM cleaned
    EXCEPT ALL
    SELECT manager_id
    FROM employees
    WHERE salary >= 144000
),
verified AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.manager_id = k.manager_id
),
with_dims AS (
    SELECT
        src.*,
        b.level,
        b.full_name AS employee_full_name,
        b.job_title AS employee_job_title
    FROM verified src
    LEFT JOIN employees b
        ON src.manager_id = b.id
),
valid AS (
    SELECT
        full_name,
        email,
        manager_id,
        job_title,
        level_filled,
        salary_scaled,
        level,
        employee_full_name,
        employee_job_title,
        CASE WHEN salary_scaled >= 68 THEN 'High' WHEN salary_scaled >= 11 THEN 'Medium' ELSE 'Low' END AS salary_scaled_level,
        COALESCE(level, 'OTHER') AS resolved_level
    FROM with_dims
    WHERE level IS NOT NULL
)
SELECT
    s.full_name,
    s.email,
    s.manager_id,
    s.job_title,
    s.salary_scaled,
    s.level,
    s.employee_full_name,
    s.employee_job_title,
    s.salary_scaled_level,
    CASE WHEN job_title RLIKE '^(Designer|Director)$' THEN 'EXTENDED' WHEN job_title RLIKE '^(Engineer|Analyst)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS job_title_zone,
    (SELECT MIN(t2.salary) FROM employees t2 WHERE t2.manager_id = s.manager_id) AS min_employee_salary
FROM valid s
ORDER BY employee_job_title NULLS FIRST, job_title DESC
