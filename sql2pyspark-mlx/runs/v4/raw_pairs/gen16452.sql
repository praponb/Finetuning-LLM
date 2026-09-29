SELECT
    level,
    SUM(salary) AS total_salary,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END) AS l2_count
FROM employees
WHERE email IS NOT NULL AND LOWER(email) LIKE '%lic%' AND hire_date <= '2024-08-13'
GROUP BY 1
ORDER BY level;
