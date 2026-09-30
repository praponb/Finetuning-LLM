WITH src AS (
    SELECT *
    FROM departments
    WHERE budget <= 2220000 AND budget < 1354000
),
DepartmentsStats AS (
    SELECT CASE WHEN budget >= 3343000 THEN 'high' WHEN budget >= 1976000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN location = 'Chiang Mai' THEN 1 ELSE 0 END) AS chiang_mai_count, ROUND(AVG(budget), 2) AS avg_budget
    FROM src
    GROUP BY CASE WHEN budget >= 3343000 THEN 'high' WHEN budget >= 1976000 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM DepartmentsStats
