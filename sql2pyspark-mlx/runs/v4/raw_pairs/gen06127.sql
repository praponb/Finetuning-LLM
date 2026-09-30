SELECT
    d.id,
    d.location,
    d.budget,
    t.stddev_salary
FROM departments d
INNER JOIN (SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM employees
    WHERE is_remote
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.stddev_salary > 62
ORDER BY d.location ASC, id ASC
LIMIT 3;
