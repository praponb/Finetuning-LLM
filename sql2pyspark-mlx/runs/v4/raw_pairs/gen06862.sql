SELECT *
FROM (
    SELECT is_remote, COUNT(CASE WHEN level = 'L1' THEN 1 END) AS l1_count, MAX(salary) AS max_salary
    FROM employees
    GROUP BY is_remote
) AS t
WHERE max_salary > 80000;
