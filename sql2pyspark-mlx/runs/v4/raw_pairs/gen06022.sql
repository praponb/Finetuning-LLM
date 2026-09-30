WITH metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS row_seq,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title, manager_id), 4) AS salary_share,
        SUM(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
    FROM employees
)
SELECT
    id,
    email,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    level,
    row_seq,
    CASE WHEN level RLIKE '^(L1|L2)$' THEN 'GROUP_A' WHEN level RLIKE '^(L3|L4)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS level_zone
FROM metrics
