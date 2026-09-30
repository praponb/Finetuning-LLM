WITH department_totals AS (
    SELECT dept_id, MIN(hire_date) first_hire_date, SUM(CASE WHEN job_title = 'Designer' THEN salary ELSE 0 END) designer_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.location, dt.first_hire_date, dt.designer_salary
FROM departments d
INNER JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.designer_salary >= 25
ORDER BY 3 ASC
