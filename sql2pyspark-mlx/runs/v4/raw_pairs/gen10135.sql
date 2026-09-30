SELECT job_title, manager_id, min_salary
FROM (
    SELECT job_title, manager_id, MIN(salary) AS min_salary
    FROM employees
    WHERE dept_id IN (3, 23)
    GROUP BY job_title, manager_id
) AS t
WHERE min_salary > 144000
ORDER BY min_salary DESC, job_title, manager_id
LIMIT 1
