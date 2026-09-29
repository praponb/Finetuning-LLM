WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY salary DESC, id ASC) = 1
)
SELECT
    s.id,
    s.email,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.level,
    (SELECT MIN(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS min_employee_salary
FROM deduped s;
