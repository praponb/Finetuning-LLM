SELECT
    is_remote,
    SUM(salary) AS total_salary,
    COUNT(*) AS cnt,
    ROUND(SUM(salary) * 100.0 / SUM(SUM(salary)) OVER (), 2) AS pct_of_total
FROM employees
WHERE email NOT LIKE '%@outlook.com'
GROUP BY is_remote
ORDER BY cnt;
