WITH matched_keys AS (
    SELECT level
    FROM employees
    INTERSECT ALL
    SELECT level
    FROM employees
    WHERE is_remote
),
verified AS (
    SELECT s.*
    FROM employees s
    INNER JOIN matched_keys k ON s.level = k.level
)
SELECT
    s.full_name,
    s.email,
    s.dept_id,
    s.salary,
    s.hire_date,
    s.level,
    CASE WHEN full_name RLIKE '^(Kanya Srisuk|Pim Chaiyaporn)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS full_name_zone,
    (SELECT SUM(e2.salary) FROM employees e2 WHERE e2.dept_id = s.dept_id) AS sum_employee_salary
FROM verified s
