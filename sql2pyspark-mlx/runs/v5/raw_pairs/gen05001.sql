SELECT hire_month, designer_salary, level_values
FROM (
    SELECT MONTH(hire_date) AS hire_month, SUM(CASE WHEN job_title = 'Designer' THEN salary ELSE 0 END) AS designer_salary, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
    FROM employees
    GROUP BY MONTH(hire_date)
) AS t
WHERE designer_salary >= 41;
