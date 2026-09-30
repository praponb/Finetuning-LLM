SELECT d.location, t.avg_salary
FROM departments d
LEFT OUTER JOIN (SELECT dept_id, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    WHERE MONTH(hire_date) IN (1, 9, 11)
    GROUP BY dept_id) t ON d.id = t.dept_id;
