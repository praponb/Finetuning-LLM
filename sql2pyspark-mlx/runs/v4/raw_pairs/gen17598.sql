SELECT
    hire_quarter,
    is_remote,
    level_values,
    total_salary
FROM (
    SELECT
        QUARTER(hire_date) AS hire_quarter,
        is_remote,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values,
        SUM(salary) AS total_salary
    FROM employees
    WHERE level = 'L2'
    GROUP BY QUARTER(hire_date), is_remote
) AS x
WHERE total_salary >= 625000;
