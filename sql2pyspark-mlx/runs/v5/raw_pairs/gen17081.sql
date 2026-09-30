SELECT
    CASE WHEN level IN ('L5', 'L4') THEN 'priority' ELSE 'other' END AS level_group,
    MIN(salary) AS min_salary,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    COUNT(DISTINCT salary) AS unique_job_titles
FROM employees
GROUP BY level_group
HAVING COUNT(DISTINCT salary) < 3
ORDER BY min_salary;
