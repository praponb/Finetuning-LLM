WITH department_totals AS (
    SELECT dept_id, SORT_ARRAY(COLLECT_SET(job_title)) job_title_list, ROUND(AVG(salary), 2) avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    departments.dept_name,
    departments.location,
    department_totals.job_title_list,
    department_totals.avg_salary
FROM departments
INNER JOIN department_totals ON departments.id = department_totals.dept_id;
