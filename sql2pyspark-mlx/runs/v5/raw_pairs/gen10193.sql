WITH filtered_employees AS (
    SELECT *, FLOOR(salary) AS salary_whole
    FROM employees
    WHERE job_title IN ('Engineer', 'Analyst') AND (level IS NULL OR dept_id IN (15, 27, 33))
),
job_title_summary AS (
    SELECT job_title, MAX(salary_whole) - MIN(salary_whole) AS salary_whole_range, AVG(salary) AS avg_salary
    FROM filtered_employees
    GROUP BY job_title
)
SELECT job_title, salary_whole_range
FROM job_title_summary
