WITH department_stats AS (
    SELECT dept_id, MAX(salary) AS max_salary, SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list
    FROM employees
    GROUP BY dept_id
)
SELECT d.location, ds.max_salary, ds.job_title_list
FROM departments d
INNER JOIN department_stats ds ON d.id = ds.dept_id
ORDER BY location;
