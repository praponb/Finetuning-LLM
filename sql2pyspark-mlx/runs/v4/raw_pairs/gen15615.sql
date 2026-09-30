SELECT job_title, avg_salary
FROM (
    SELECT job_title, ROUND(AVG(salary), 2) AS avg_salary
    FROM employees
    WHERE level = 'L3'
    GROUP BY job_title
) AS x
WHERE avg_salary >= 70
ORDER BY job_title, avg_salary DESC
LIMIT 10;
