SELECT
    level,
    CASE WHEN salary >= 139000 THEN 'high' WHEN salary >= 48000 THEN 'mid' ELSE 'low' END salary_bucket,
    MAX(hire_date) last_hire_date,
    MIN(salary) min_salary
FROM employees
WHERE LOWER(full_name) LIKE '%vy %'
GROUP BY level, CASE WHEN salary >= 139000 THEN 'high' WHEN salary >= 48000 THEN 'mid' ELSE 'low' END
ORDER BY min_salary DESC, last_hire_date DESC;
