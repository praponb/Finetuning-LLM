WITH enriched AS (
    SELECT
        *,
        LAG(salary, 1) OVER (ORDER BY hire_date ASC, id ASC) AS prev_salary,
        AVG(salary) OVER (PARTITION BY manager_id) AS manager_id_avg_salary
    FROM employees
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
    CASE WHEN job_title RLIKE '^(Analyst|Intern)$' THEN 'OTHER_ZONE' WHEN job_title RLIKE '^(Engineer|Manager)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS job_title_zone,
    (SELECT MIN(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS min_employee_salary
FROM enriched s
