WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, manager_id ORDER BY salary DESC, id) AS dedup_rank
    FROM employees
    QUALIFY dedup_rank = 1
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.job_title,
    s.salary,
    s.is_remote,
    s.level,
    CASE WHEN level RLIKE '^(L4|L1)$' THEN 'CORE' WHEN level RLIKE '^(L2|L3)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS level_zone,
    (SELECT SUM(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS sum_employee_salary
FROM deduped s
ORDER BY id DESC NULLS LAST, job_title
