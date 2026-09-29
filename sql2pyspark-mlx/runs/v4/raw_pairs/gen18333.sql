SELECT d.budget, t.job_title_list, t.first_hire_date
FROM departments d
INNER JOIN (SELECT dept_id, SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list, MIN(hire_date) AS first_hire_date
    FROM employees
    WHERE level <> 'L4'
    GROUP BY dept_id) t ON d.id = t.dept_id;
