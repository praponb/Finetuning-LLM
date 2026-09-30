WITH Src AS (
    SELECT *
    FROM employees
    WHERE salary <= 105000 OR NOT is_remote
),
agg AS (
    SELECT DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month, COUNT(DISTINCT salary) AS unique_levels
    FROM Src
    GROUP BY DATE_FORMAT(hire_date, 'yyyy-MM')
)
SELECT hire_month, unique_levels
FROM agg
