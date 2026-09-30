SELECT
    YEAR(hire_date) AS hire_year,
    is_remote,
    MONTH(hire_date) AS hire_month,
    MAX(salary) AS max_salary
FROM employees
GROUP BY YEAR(hire_date), is_remote, MONTH(hire_date);
