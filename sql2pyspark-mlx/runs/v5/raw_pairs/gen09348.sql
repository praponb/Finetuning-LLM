SELECT
    hire_year,
    is_remote,
    l3_count,
    min_salary
FROM (
    SELECT
        YEAR(hire_date) AS hire_year,
        is_remote,
        SUM(CASE WHEN level = 'L3' THEN 1 ELSE 0 END) AS l3_count,
        MIN(salary) AS min_salary
    FROM employees
    GROUP BY YEAR(hire_date), is_remote
) AS s
WHERE min_salary < 78000
ORDER BY 1 DESC, is_remote
LIMIT 50;
