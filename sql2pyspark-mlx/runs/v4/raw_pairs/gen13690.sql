SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY level ORDER BY salary DESC) AS rnk
    FROM employees
) AS r
WHERE rnk = 1
