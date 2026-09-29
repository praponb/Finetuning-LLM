SELECT DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month, SORT_ARRAY(COLLECT_SET(level)) AS level_list, COUNT(*) AS num_employees
FROM employees
GROUP BY 1
HAVING num_employees >= 5
ORDER BY 1 DESC
