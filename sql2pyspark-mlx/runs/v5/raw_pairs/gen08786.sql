WITH remaining_keys AS (
    SELECT id, dept_id
    FROM employees
    EXCEPT DISTINCT
    SELECT id, dept_id
    FROM employees
    WHERE dept_id = 14
),
verified AS (
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.dept_id = k.dept_id
),
enriched AS (
    SELECT a.*, d.dept_name
    FROM verified a
    LEFT JOIN departments d
        ON a.dept_id = d.id
    WHERE d.budget > 3145000
),
with_dims AS (
    SELECT
        e.email,
        e.dept_id,
        e.manager_id,
        e.hire_date,
        e.is_remote,
        e.dept_name,
        d.level AS employee_level
    FROM enriched e
    LEFT JOIN employees d
        ON e.manager_id = d.id
),
with_dims_employees AS (
    SELECT
        s.email,
        s.dept_id,
        s.hire_date,
        s.dept_name,
        d.budget,
        d.location
    FROM with_dims s
    LEFT JOIN departments d
        ON s.dept_id = d.id
)
SELECT
    s.dept_id,
    s.hire_date,
    s.dept_name,
    s.budget,
    CASE WHEN dept_name RLIKE '^(Sales|HR)$' THEN 'PRIORITY' WHEN dept_name RLIKE '^(Engineering|Marketing)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS dept_name_zone,
    (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept_id = s.dept_id) AS avg_employee_salary
FROM with_dims_employees s
ORDER BY dept_name_zone DESC NULLS FIRST, dept_id;
