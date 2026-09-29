SELECT
    d.id,
    d.budget,
    t.stddev_salary,
    t.job_title_values
FROM departments d
INNER JOIN (SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.stddev_salary > 31
ORDER BY job_title_values, id
LIMIT 5;
