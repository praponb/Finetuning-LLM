SELECT
    YEAR(hire_date) hire_year,
    MAX(hire_date) latest_hire_date,
    SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END) l4_count,
    MIN(salary) min_salary
FROM employees
GROUP BY YEAR(hire_date)
HAVING SUM(salary) < 482000
ORDER BY hire_year DESC;
