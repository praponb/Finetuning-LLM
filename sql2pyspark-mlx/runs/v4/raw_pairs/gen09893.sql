SELECT
    CASE WHEN salary >= 119000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    ROUND(AVG(salary), 2) AS avg_salary
FROM employees
GROUP BY CASE WHEN salary >= 119000 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING avg_salary >= 0;
