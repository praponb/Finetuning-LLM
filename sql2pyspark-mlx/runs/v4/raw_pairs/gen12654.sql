SELECT
    CASE WHEN job_title IN ('Engineer', 'Analyst') THEN 'group_a' ELSE 'other' END job_title_group,
    CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END hire_cohort,
    MIN(hire_date) first_hire_date,
    COUNT(CASE WHEN job_title = 'Director' THEN 1 END) director_count
FROM employees
GROUP BY job_title_group, hire_cohort
HAVING director_count < 1;
