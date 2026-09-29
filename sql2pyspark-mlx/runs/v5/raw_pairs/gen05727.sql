SELECT level, max_salary, engineer_count
FROM (
    SELECT level, MAX(salary) AS max_salary, SUM(CASE WHEN job_title = 'Engineer' THEN 1 ELSE 0 END) AS engineer_count
    FROM employees
    GROUP BY level
) AS agg
WHERE max_salary < 119000
