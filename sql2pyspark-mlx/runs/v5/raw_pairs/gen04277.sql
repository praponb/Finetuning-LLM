WITH remaining_keys AS (
    SELECT level
    FROM employees
    EXCEPT DISTINCT
    SELECT level
    FROM employees
    WHERE level IN ('L2', 'L5', 'L3')
),
kept_rows AS (
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.level = k.level
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.salary,
    s.is_remote,
    s.level,
    CASE WHEN level RLIKE '^(L1|L4)$' THEN 'EXTENDED' WHEN level RLIKE '^(L2|L3)$' THEN 'GROUP_B' ELSE 'CORE' END AS level_zone,
    (SELECT MIN(e2.salary) FROM employees e2 WHERE e2.dept_id = s.dept_id) AS min_employee_salary
FROM kept_rows s
ORDER BY min_employee_salary NULLS FIRST
