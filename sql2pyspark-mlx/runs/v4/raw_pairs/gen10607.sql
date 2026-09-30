SELECT
    QUARTER(hire_date) AS hire_quarter,
    is_remote,
    MAX(hire_date) AS last_hire_date,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
FROM employees
WHERE email NOT LIKE '%@GMAIL.COM'
GROUP BY QUARTER(hire_date), is_remote
