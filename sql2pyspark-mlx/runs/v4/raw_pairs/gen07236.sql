WITH enriched AS (
    SELECT
        a.*,
        d.salary AS employee_salary,
        d.hire_date AS employee_hire_date,
        d.full_name AS employee_full_name
    FROM employees a
    LEFT JOIN employees d
        ON a.manager_id = d.id
)
SELECT
    id,
    full_name,
    email,
    manager_id,
    job_title,
    hire_date,
    level,
    employee_salary,
    employee_hire_date
FROM enriched
ORDER BY id DESC NULLS LAST
LIMIT 5
