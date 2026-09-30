WITH valid_employees AS (
    SELECT *
    FROM employees
    WHERE YEAR(hire_date) = 2024
),
employees_stats AS (
    SELECT is_remote, MAX(salary) AS max_salary, SORT_ARRAY(COLLECT_SET(level)) AS level_list
    FROM valid_employees
    GROUP BY is_remote
)
SELECT *
FROM employees_stats
WHERE max_salary >= 95000
ORDER BY max_salary DESC, is_remote ASC
LIMIT 5;
