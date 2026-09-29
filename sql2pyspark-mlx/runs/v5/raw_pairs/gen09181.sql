SELECT *
FROM (
    SELECT
        id,
        job_title,
        salary,
        RANK() OVER (PARTITION BY job_title ORDER BY salary DESC) AS rnk
    FROM employees
) AS t
WHERE rnk <= 5
ORDER BY job_title DESC;
