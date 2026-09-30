SELECT
    CASE WHEN salary >= 122000 THEN 'high' WHEN salary >= 41000 THEN 'mid' ELSE 'low' END AS salary_bucket,
    DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month,
    is_remote,
    AVG(salary) AS avg_salary
FROM employees
GROUP BY 1, 2, 3
HAVING avg_salary < 110000;
