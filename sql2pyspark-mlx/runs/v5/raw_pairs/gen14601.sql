SELECT
    CASE WHEN salary >= 164000 THEN 'high' WHEN salary >= 52000 THEN 'mid' ELSE 'low' END salary_bucket,
    dept_id,
    MIN(salary) min_salary,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) job_title_values
FROM employees
WHERE UPPER(TRIM(email)) = 'ALICE@GMAIL.COM'
GROUP BY 1, 2
HAVING MIN(salary) > 104000
ORDER BY min_salary DESC;
