SELECT
    job_title,
    CASE WHEN salary >= 127000 THEN 'high' WHEN salary >= 63000 THEN 'mid' ELSE 'low' END AS salary_bucket,
    SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END) AS l4_count,
    MIN(salary) AS min_salary
FROM employees
GROUP BY job_title, CASE WHEN salary >= 127000 THEN 'high' WHEN salary >= 63000 THEN 'mid' ELSE 'low' END
