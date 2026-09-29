SELECT
    level,
    SUM(salary) AS total_salary,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(salary) DESC) AS rnk
FROM employees
GROUP BY level;
